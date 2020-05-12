import subprocess
from functools import partial
import os, sys
sys.path.append(os.getcwd())

import toml
# from dask.highlevelgraph import HighLevelGraph

from dask_jobqueue import SLURMCluster

# class WorkflowParser:
#
#     def __init__(self, workflow_filepath):
#         self.workflow_filepath = workflow_filepath
#
#     def parse_workflow(self):

def parse_step(step):
    """

    Parameters
    ----------
    step : dict
        contains the info for the step (dependencies, bash scripts, etc)

    Returns
    -------

    """

def workflow_to_dsk(workflow_filepath:str) -> dict:
    """Parses workflow to Dask High Level Graph

    Parameters
    ----------
    workflow_filepath : str
        TOML file defining the workflow

    Returns
    -------
    out : dask.HighLevelGraph
    """
    run = partial(subprocess.run, shell=True)

    workflow = toml.load(workflow_filepath)

    dsk = {}
    for job_key, workflow_job in workflow.items():
        if job_key == 'cluster': # all top level sections are jobs except for the 'cluster' section
            continue
        task_dependencies = None
        if 'depends' in workflow_job:
            job_dependencies = list(set(workflow_job['depends']))
            task_dependencies = [(job_dep, task_dep) for job_dep in job_dependencies for task_dep in workflow[job_dep]['tasks']]
        for task_key, task_script in workflow_job['tasks'].items():
            graph_task = (run, task_script)
            if task_dependencies:
                graph_task = [graph_task] + task_dependencies
            
            dsk[(job_key, task_key)] = graph_task

    return dsk

if __name__ == "__main__":
    from distributed import Client, LocalCluster
    cluster = LocalCluster(threads_per_worker=2, n_workers=3, processes=False)
    client = Client(cluster)
    
    dsk = workflow_to_dsk('workflow_to_graph/workflow_example_v3.toml')
    result = client.get(dsk, list(dsk.keys())[::-1])
    print('hi')
