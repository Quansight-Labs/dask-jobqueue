import pytest 
from distributed import Client, get_worker
from functools import partial
import subprocess
import dask 

@pytest.mark.env("mpc")
def test_scale_single_spec(mpc_cluster):
    mpc = mpc_cluster
    for i in [1, 3, 1, 0]:
        mpc.scale(i, spec_name="small")
        assert len(mpc.get_workers_local('small')) == i


@pytest.mark.env("mpc")
def test_scale_two_spec(mpc_cluster):
    mpc = mpc_cluster
    mpc.scale(1, spec_name='small')
    assert len(mpc.get_workers_local('small')) == 1
    
    mpc.add_new_spec('large', cores=2, memory='250 MB')

    mpc.scale(2, spec_name='small')
    assert len(mpc.get_workers_local('small')) == 2
    mpc.scale(1, spec_name='large')
    assert len(mpc.get_workers_local('large')) == 1
    for spec_name in mpc.named_specs.keys():
        mpc.scale(0, spec_name=spec_name) 
        assert len(mpc.get_workers_local(spec_name)) == 0

@pytest.mark.env("mpc")
def test_get_all_specs(mpc_cluster):
    mpc = mpc_cluster
    assert len(mpc.named_specs) == 2


# helper for this example
def make_bash_list(job_num, ntasks):
    bash_list = []
    for i in range(ntasks):
        bash_list.append(f"echo 'job{job_num}-task{i}' && "
                         f"""touch "job{job_num}-task{i}-$(date +%T)" && """
                         "sleep 1")
    return bash_list


def test_run_task_on_specific_worker(mpc_cluster):
    mpc = mpc_cluster
    mpc.add_new_spec('large', cores=2, memory='250 MB')
    mpc.scale(1, spec_name='small')
    small_worker_names = mpc.get_workers_local('small')
    mpc.scale(1, spec_name='large')   
    large_worker_names = mpc.get_workers_local('large')

    client = Client(mpc)
    
    # make bash scripts for each job
    job0_bash_list = make_bash_list(0, 2)
    job1_bash_list = make_bash_list(1, 1)
    job2a_bash_list = make_bash_list('2a', 3)
    job2b_bash_list = make_bash_list('2b', 3)

    # define job_runner
    run = partial(subprocess.run, shell=True)
    def job_runner(bash_list):
        for task in bash_list:
            run(task)
        return get_worker().name

    dsk = {
        # can be specified in any order
        'job2b': [(job_runner, job2b_bash_list), 'job1'],
        'job2a': [(job_runner, job2a_bash_list), 'job1'],
        'job1': [(job_runner, job1_bash_list), 'job0'],
        'job0': (job_runner, job0_bash_list),
    }
    
    results = client.get(dsk, sorted([key for key in dsk]),
                        resources={'job0': {'small': 1},
                                    'job1': {'large': 1},
                                    'job2a': {'small': 1},
                                    'job2b': {'large': 1}}
                        ) 
    assert results[0] in small_worker_names
    assert results[1][0] in large_worker_names
    assert results[2][0] in small_worker_names
    assert results[3][0] in large_worker_names
    
    # Clean Up
    for spec in ['large', 'small']:
        mpc.scale(0, spec_name=spec)