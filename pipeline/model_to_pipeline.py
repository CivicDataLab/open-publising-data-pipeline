from datatransform.models import Pipeline
from pipeline import pipeline
import time
from pipeline.add import add
from projects.dpg_pipeline.mgnrega_flow.mgnrega_flow import mgnrega_pipeline
#from pipeline.mgnrega_flow import mgnrega_pipeline
from projects.generic_flow.generic_transformation_tasks import prefect_tasks
#from projects.dpg_pipeline.mgnrega_flow import *

def task_executor(pipeline_id, data_url, project):
    print("inside te***")
    print("pipeline_id is ", pipeline_id)
    time.sleep(5)
    try:
    #     data = None
    #     try:
    #         data = pd.read_csv(data_pickle)
    #         print(data)
    #     except Exception as e:
    #         print(str(e), "error in model to pipeline!!!!!")
    #         pass
    #     finally:
    #         os.remove(data_pickle)
        print(" got pipeline id...", pipeline_id)
        pipeline_object = Pipeline.objects.get(pk=pipeline_id)
        tasks = pipeline_object.task_set.all().order_by("order_no")
        new_pipeline = pipeline.Pipeline(pipeline_object, data_url)
        print("received tasks from POST request..for..", new_pipeline.model.pipeline_name)
        # print("data before...", new_pipeline.data)
        def execution_from_model(task):
            print("adding tasks!!")
            new_pipeline.add(task)

        [execution_from_model(task) for task in tasks]
        if project == "generic_transformations":
            prefect_tasks.pipeline_executor(new_pipeline)  # pipeline_executor(task.task_name, context)
        elif project == "dpg_mgnrega":
            print("inside elif!!")
            print(add(4,5))
            mgnrega_pipeline(new_pipeline)
        return

    except Exception as e:
        raise e
