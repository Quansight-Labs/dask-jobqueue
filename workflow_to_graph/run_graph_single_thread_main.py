import subprocess
from functools import partial

from distributed import Client, LocalCluster, Variable
from dask_jobqueue import SLURMCluster  # will need to import programatically at some point

# helper for this example
def make_bash_list(job_num, ntasks):
    bash_list = []
    for i in range(ntasks):
        bash_list.append(f"echo 'job{job_num}-task{i}' && "
                         f"touch 'job{job_num}-task{i}' && "
                         "sleep 5")
    return bash_list

if __name__ == '__main__':

    # define job_runner
    run = partial(subprocess.run, shell=True)
    def job_runner(bash_list, worker_profile, existing_clusters):

        # read cluster from global variable
        prior_clusters = existing_clusters.get()
        if worker_profile in prior_clusters:
            client = prior_clusters[worker_profile]
            cluster = client.cluster
            # cluster.scale(1)
        # else:
            # cluster = SLURMCluster(worker_profile)
            # cluster = LocalCluster(**worker_profile)  # these would be SLURMClusters in real usage
            # existing_clusters.set(existing_clusters.get().update({worker_profile: cluster}))
            # client = Client(cluster)

        stdout_string_futures = client.map(run, bash_list)
        results = client.gather(stdout_string_futures)
        # cluster.scale(0)
        return results

    # make bash scripts for each job
    job0_bash_list = make_bash_list(0, 2)
    job1_bash_list = make_bash_list(1, 1)
    job2_bash_list = make_bash_list(2, 3)

    # parse toml file (not shown) to graph like below

    # Define single task cluster types
    small_cluster_profile = {
        # 'cluster_type': 'SLURMCluster',
        'n_workers': 1,
        'threads_per_worker': 2,
        'memory_limit': '1 GB',
        'processes': False,
        'dashboard_address': ":8788"
    }

    large_cluster_profile = { # we only want this to spin up once dependencies are completed (until job2 is ready to start)
        # 'cluster_type': 'SLURMCluster',
        'n_workers': 4,
        'threads_per_worker': 2,
        'memory_limit': '2 GB',
        'processes': True,
        'dashboard_address': ":8789"
    }


    # set up minimal cluster and main client
    cluster = LocalCluster(n_workers=1, threads_per_worker=4, memory_limit='5 GB', processes=False)
    small_SLURM_cluster = LocalCluster(**small_cluster_profile)
    large_SLURM_cluster = LocalCluster(**large_cluster_profile)

    # from time import sleep
    # sleep(5) # this just gives me time to refresh the dashboards in my browser

    # mclient is the main client
    mclient = Client(cluster)
    small_client = Client(small_SLURM_cluster)
    large_client = Client(large_SLURM_cluster)

    existing_clients = Variable('existing_clients', mclient)
    existing_clients.set({'small_cluster': small_client,
                          'large_cluster': large_client
                          })

    dsk = {
        'job0': (job_runner, job0_bash_list, 'small_cluster', existing_clients),
        'job1': [(job_runner, job1_bash_list, 'small_cluster', existing_clients), 'job0'],
        'job2': [(job_runner, job2_bash_list, 'large_cluster', existing_clients), 'job1']
    }
    # dsk = {}
    # {f'job{i}': i for i in range(3, 100)}
    dsk = {**dsk, **{f'job{i}': [(job_runner, job1_bash_list, 'large_cluster', existing_clients), 'job1']
                   for i in range(3, int(1e4))}}


    # results = mclient.get(dsk, [key for key in dsk])
    import dask
    results = dask.threaded.get(dsk, [key for key in dsk])
    print('bye')

    # look at job_runner to see what happens as each job is executed.