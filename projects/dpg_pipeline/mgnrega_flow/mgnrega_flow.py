from prefect import task, flow
from task_utils import *



@task
def mgnrega_scraper(context, pipeline, task_obj):
    print("inside scraper publisher!")
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:    # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        #df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        print(data)
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
        send_info_to_prefect_cloud("MGNREGA scraper task finished successfully")
    else:
       pipeline.logger.error("ERROR: at mgnrega_scraper")


@task
def mgnrega_transformer(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:    # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        #df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        send_info_to_prefect_cloud("MGNREGA transforer task finished successfully! Resultant file can be found at- " + pipeline.data_path + " on the server")
        set_task_model_values(task_obj, pipeline)
    else:
       pipeline.logger.error("ERROR: at mgnrega_transformer")


@flow
def mgnrega_pipeline(pipeline):
    print("setting ", pipeline.model.pipeline_name, " status to In Progress")
    pipeline.model.status = "In Progress"
    pipeline.logger.info(f"""INFO: setting pipeline status to - In Progress""")
    print(pipeline.model.status)
    pipeline.model.save()
    tasks_objects = pipeline._commands
    func_names = get_task_names(tasks_objects)
    contexts = get_task_contexts(tasks_objects)
    try:
        for i in range(len(func_names)):
            globals()[func_names[i]](contexts[i], pipeline, tasks_objects[i])
    except Exception as e:
        pipeline.logger.error(f"""ERROR: Flow failed with an error - {str(e)}""")
        raise e
    for task in tasks_objects:
        if task.status == "Failed":
            pipeline.model.status = "Failed"
            pipeline.logger.info(f"""INFO: The task - {task.task_name} was failed. Set Pipeline status to failed.""")
            pipeline.model.save()
            break
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.logger.info(f"""INFO: Set Pipeline status to Done.""")
        pipeline.model.save()
    pipeline.model.output_id = str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    # print("Data after pipeline execution\n", pipeline.data)
    return


