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


class MultiPoolCluster(SLURMCluster):
    job_cls = MultiPoolJob
    config_name = "slurm"

    # If I just end up passing worker_pools to MultiPoolJob then I don't even need this __init__, and I can inherit from SlurmCluster
    def __init__(
        self,
        spec_name = None,
        **kwargs
    ):
        if 'extra' in kwargs:
            raise NotImplementedError("There is currently no way to pass in variables besides "
                                    "the spec_name.  Use spec_name parameter for that purpose."
            )
        else:
            if not spec_name:
                spec_name = 'default'
            kwargs['extra'] = ['--resources', spec_name]
        super().__init__(**kwargs)
        self.named_specs = {}
        self.add_new_spec(spec_name, **self.new_spec)

    def add_new_spec(self, name, **kwargs):
        # user can't pass in other extra settings, but should be okay for my use case
        new_kwargs = {**self._kwargs, **kwargs, **{'extra': ['--resources', name + "=1"]}}
        worker = {"cls": self.job_cls, "options": new_kwargs}
        self.named_specs[name] = worker
        return 

    def get_workers_local(self, profile_name):
        workers = []
        for name, worker_spec in self.worker_spec.items():           
            check_next = False
            for extra_elem in worker_spec['options']['extra']:
                if check_next:
                    if extra_elem.split('=')[0] == profile_name:
                        workers.append(name)
                    check_next = False
                else:
                    if extra_elem == '--resources':
                        check_next = True
        return workers


    def get_workers_scheduler(self, profile_name):
        workers = []
        for worker in self.scheduler_info['workers'].values():
            if profile_name in worker['resources']:
                workers.append(worker['name'])
        return workers

    # def scale(self, n=None, jobs=0, memory=None, cores=None):
    #     """ Scale cluster to specified configurations.

    #     Parameters
    #     ----------
    #     n : int or dict
    #        Target number of workers
    #     jobs : int
    #        Target number of jobs
    #     memory : str
    #        Target amount of memory
    #     cores : int
    #        Target number of cores

    #     Notes: The logic is, set
    #     """
        

    #     if isinstance(n, dict):
    #         cum_n_workers = len(self.workers)
    #         scaling_dict = n
    #         for profile, n_workers in scaling_dict.items():
    #             cum_n_workers += n_workers
    #             self.new_spec = sample_worker_spec[profile]
    #             self.scale(n=cum_n_workers)
    #     else:
    #         self.scale(n=None,
    #                      jobs=jobs,
    #                      memory=memory,
    #                      cores=cores)

    def scale(self, n=None, jobs=0, spec_name=None):
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
        # if spec_name is None:
        #     raise InputException('Please define spec_name')

        if n is not None:
            jobs = int(math.ceil(n / self._dummy_job.worker_processes))
            n = jobs

        # if memory is not None:
        #     n = max(n, int(math.ceil(parse_bytes(memory) / self._memory_per_worker())))

        # if cores is not None:
        #     n = max(n, int(math.ceil(cores / self._threads_per_worker())))

        # remove workers if needed
        local_workers = self.get_workers_local(spec_name)
        if len(local_workers) > n:
            not_yet_launched = set(local_workers) - \
                                set(self.get_workers_scheduler(spec_name))

            # not_yet_launched = set(self.worker_spec) - {
            #     v["name"] for v in self.scheduler_info["workers"].values()
            # }
            while len(self.get_workers_local(spec_name)) > n and not_yet_launched:
                del self.worker_spec[not_yet_launched.pop()]
        
        while len(self.get_workers_local(spec_name)) > n:
            # need to pop from self.worker_spec
            # Maybe I should keep a list that keeps track of which workers are of which profile so
            # I'm not recalculating this all the time
            del self.worker_spec[self.get_workers_local(spec_name)[-1]]

        # add workers if needed
        if self.status not in ("closing", "closed"):
            while len(self.get_workers_local(spec_name)) < n:
                self.new_spec = self.named_specs[spec_name]
                self.worker_spec.update(self.new_worker_spec())

        self.loop.add_callback(self._correct_state)

        if self.asynchronous:
            return NoOpAwaitable()