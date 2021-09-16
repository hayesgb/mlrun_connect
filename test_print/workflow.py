from mlrun import new_project, NewTask
from kfp import dsl

funcs = {}

@dsl.pipeline(
    name = "test print pipeline",
    description = "testing 0.6.3"
)
def kfpipeline():
    printpod = funcs['test-print'].as_step(NewTask(handler="test_print"))
    
