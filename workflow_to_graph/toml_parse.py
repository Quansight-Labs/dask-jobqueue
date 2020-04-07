import subprocess
from functools import partial

import toml
from dask.highlevelgraph import HighLevelGraph

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

def workflow_to_HLG(workflow_filepath:str) -> HighLevelGraph:
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

    layer_keys = [k for k in workflow if k != 'resources']

    layers = {}
    dependencies = {}
    for layer_key in layer_keys:
        workflow_step = workflow[layer_key]
        if 'depends' in workflow[layer_key]:
            dependencies[layer_key] = set(workflow_step['depends'])
        else:
            dependencies[layer_key] = set()

        layer = {}
        for task_name, task in workflow_step['tasks'].items():
            layer[(layer_key, task_name)] = (run, task)

        layers[layer_key] = layer

    return HighLevelGraph(layers=layers, dependencies=dependencies)

if __name__ == "__main__":
    from distributed import Client
    client = Client(processes=1)

    dsk = workflow_to_HLG('workflow_example.toml')
    result = client.get(dsk, ('job2', 'task1'))
    print('hi')
