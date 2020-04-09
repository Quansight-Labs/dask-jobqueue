# Based on the idea https://github.com/dask/dask-jobqueue/issues/378
import asyncio
import atexit
import copy
import logging
import math
import weakref

import dask
from tornado import gen

from distributed.deploy.adaptive import Adaptive
from distributed.deploy.cluster import Cluster
from distributed.core import rpc, CommClosedError
from distributed.utils import (
    LoopRunner,
    silence_logging,
    ignoring,
    parse_bytes,
    parse_timedelta,
    import_term,
    TimeoutError,
)
from distributed.scheduler import Scheduler
from distributed.security import Security
from distributed.deploy.spec import NoOpAwaitable

import logging
import math

import dask
from distributed import Scheduler

from dask_jobqueue import JobQueueCluster, SLURMCluster
from dask_jobqueue.core import Job
from dask_jobqueue.slurm import SLURMJob


logger = logging.getLogger(__name__)


class MultiPoolJob(SLURMJob):
    # Override class variables
    submit_command = "sbatch"
    cancel_command = "scancel"
    config_name = "slurm"
    pass


class MultiPoolCluster(SLURMCluster):
    job_cls = MultiPoolJob
    config_name = "slurm"

    # If I just end up passing worker_pools to MultiPoolJob then I don't even need this __init__, and I can inherit from SlurmCluster
    # def __init__(
    #     self,
    #     worker_pools=None,
    #     **kwargs
    # ):
    #     self.worker_pools = worker_pools
    #     super().__init__(**kwargs)

    def scale(self, n=None, jobs=0, memory=None, cores=None):
        """ Scale cluster to specified configurations.

        Parameters
        ----------
        n : int or dict
           Target number of workers
        jobs : int
           Target number of jobs
        memory : str
           Target amount of memory
        cores : int
           Target number of cores

        Notes: The logic is, set
        """
        if n is not None:
            jobs = int(math.ceil(n / self._dummy_job.worker_processes))
            n = jobs

        if memory is not None:
            n = max(n, int(math.ceil(parse_bytes(memory) / self._memory_per_worker())))

        if cores is not None:
            n = max(n, int(math.ceil(cores / self._threads_per_worker())))

        # remove workers if needed
        if len(self.worker_spec) > n:
            not_yet_launched = set(self.worker_spec) - {
                v["name"] for v in self.scheduler_info["workers"].values()
            }
            while len(self.worker_spec) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]

        while len(self.worker_spec) > n:
            self.worker_spec.popitem()

        # add workers if needed
        if self.status not in ("closing", "closed"):
            while len(self.worker_spec) < n:
                self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()





class SGEIdiapCluster(JobQueueCluster):
    """ Launch Dask jobs in the IDIAP SGE cluster

    """

    def __init__(self, **kwargs):

        # Defining the job launcher
        self.job_cls = SGEIdiapJob

        # we could use self.workers to could the workers
        # However, this variable works as async, hence we can't bootstrap
        # several cluster.scale at once
        self.n_workers_sync = 0

        # Hard-coding some scheduler info from the time being
        self.protocol = "tcp://"
        silence_logs = "error"
        dashboard_address = ":8787"
        interface = None
        host = None
        security = None

        scheduler = {
            "cls": Scheduler,  # Use local scheduler for now
            "options": {
                "protocol": self.protocol,
                "interface": interface,
                "host": host,
                "dashboard_address": dashboard_address,
                "security": security,
            },
        }

        # Spec cluster parameters
        loop = None
        asynchronous = False
        name = None

        # Starting the SpecCluster constructor
        super(JobQueueCluster, self).__init__(
            scheduler=scheduler,
            worker={},
            loop=loop,
            silence_logs=silence_logs,
            asynchronous=asynchronous,
            name=name,
            **kwargs
        )


    def scale(self, n_jobs, queue=None, memory="4GB", io_big=True, resources=None):
        """
        Launch an SGE job in the Idiap SGE cluster


        Parameters
        ----------

          n_jobs: int
            Number of jobs to be launched

          queue: str
            Name of the SGE queue

          io_big: bool
            Use the io_big? Note that this is only true for q_1day, q1week, q_1day_mth, q_1week_mth

          resources: str
            Tag your workers with meaningful name (e.g GPU=1). In this way, it's possible to redirect certain tasks to certain workers.

        """

        if n_jobs == 0:
            # Shutting down all workers
            return super(JobQueueCluster, self).scale(0, memory=None, cores=0)

        resource_spec = f"{queue}=TRUE"  # We have to set this at Idiap for some reason
        resource_spec += ",io_big=TRUE" if io_big else ""
        log_directory = "./logs"
        n_cores = 1
        worker_spec = {
            "cls": self.job_cls,
            "options": {
                "queue": queue,
                "memory": memory,
                "cores": n_cores,
                "processes": n_cores,
                "log_directory": log_directory,
                "local_directory": log_directory,
                "resource_spec": resource_spec,
                "interface": None,
                "protocol": self.protocol,
                "security": None,
                "resources": resources,
            },
        }

        # Defining a new worker_spec with some SGE characteristics
        self.new_spec = worker_spec # I think I need to replace with self.new_worker_spec

        # Launching the jobs according to the new worker_spec
        n_workers = self.n_workers_sync
        self.n_workers_sync += n_jobs
        return super(JobQueueCluster, self).scale(
            n_workers + n_jobs, memory=None, cores=n_cores)




class SGEIdiapJob(Job):
    """
    Launches a SGE Job in the IDIAP cluster.
    This class basically encodes the CLI command that bootstrap the worker
    in a SGE job. Check here `https://distributed.dask.org/en/latest/resources.html#worker-resources` for more information

    ..note: This is class is temporary. It's basically a copy from SGEJob from dask_jobqueue.
            The difference is that here I'm also handling the dask job resources tag (which is not handled anywhere). This has to be patched in the Job class. Please follow here `https://github.com/dask/dask-jobqueue/issues/378` to get news about this patch


    """
    submit_command = "qsub"
    cancel_command = "qdel"

    def __init__(
            self,
            *args,
            queue=None,
            project=None,
            resource_spec=None,
            walltime=None,
            job_extra=None,
            config_name="sge",
            **kwargs
    ):
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % config_name)
        if resource_spec is None:
            resource_spec = dask.config.get("jobqueue.%s.resource-spec" % config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)

        super().__init__(*args, config_name=config_name, **kwargs)

        # Amending the --resources in the `distributed.cli.dask_worker` CLI command
        if "resources" in kwargs and kwargs["resources"]:
            resources = kwargs["resources"]
            self._command_template += f" --resources {resources}"

        header_lines = []
        if self.job_name is not None:
            header_lines.append("#$ -N %(job-name)s")
        if queue is not None:
            header_lines.append("#$ -q %(queue)s")
        if project is not None:
            header_lines.append("#$ -P %(project)s")
        if resource_spec is not None:
            header_lines.append("#$ -l %(resource_spec)s")
        if walltime is not None:
            header_lines.append("#$ -l h_rt=%(walltime)s")
        if self.log_directory is not None:
            header_lines.append("#$ -e %(log_directory)s/")
            header_lines.append("#$ -o %(log_directory)s/")
        header_lines.extend(["#$ -cwd", "#$ -j y"])
        header_lines.extend(["#$ %s" % arg for arg in job_extra])
        header_template = "\n".join(header_lines)

        config = {
            "job-name": self.job_name,
            "queue": queue,
            "project": project,
            "processes": self.worker_processes,
            "walltime": walltime,
            "resource_spec": resource_spec,
            "log_directory": self.log_directory,
        }
        self.job_header = header_template % config
        logger.debug("Job script: \n %s" % self.job_script())

if __name__ == "__main__":
    # sc = SLURMCluster()
    mpc = MultiPoolCluster(cores=1, memory='200 MB')
    mpc.scale(1)
    print('bye')