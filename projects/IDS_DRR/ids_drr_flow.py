from prefect import task, flow
from task_utils import *


@task
def collect_imd_data(context, pipeline, task_obj):
    """
    Input - year in YYYY format

    context would look like -
    {
        "year": "2022"
    }

    Output - creates data folders on local. Doesn't return anything
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at collect_imd_data")


@task
def sentinel_upload_to_s3(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at sentinel_upload_to_s3")


@task
def sentinel(context, pipeline, task_obj):
    """
    Input - date_start in yyyy-MM-dd format
            date_end in yyyy-MM-dd format
    context would look like the following
    {
        "date_start": "2023-01-01"
        "date_end": "2023-06-01"
    }

    Output - The task creates files and folders in the server. Nothing is returned.
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at sentinel")


@task
def bhuvan_get_dates(context, pipeline, task_obj):
    """
    Input - No input
    Output - Dates the images are available on BHUVAN
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data  # dates
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at bhuvan_get_dates")


@task
def bhuvan_gdal_wms(context, pipeline, task_obj):
    """
    Input - dates from get_date. Dates will be taken from pipeline.data_path variable
    Output - Downloads images to local. No need to return anything from the task
    """

    context = context.update({'dates': pipeline.data_path})  # update the context with dates got from - get_dates
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at bhuvan_gdal_wms")


@task
def bhuvan_remove_watermark(context, pipeline, task_obj):
    """
    Input - Nothing. Remove watermarks from all the files
    Output - Saves the files in local. Returns nothing
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at bhuvan_remove_watermark")


@task
def bhuvan_transformer(context, pipeline, task_obj):
    """
    Input - Year and month - Format - YYYY, MM. Sent by API call in the context in the following format
            {
                "year": "YYYY",
                "month": "MM"
            }
    Output - The task removes watermarks from the images. Returns nothing
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at bhuvan_transformer")


@task
def bhuvan_upload_to_s3(context, pipeline, task_obj):
    """
    Uploads all the data scraped and transformed from BHUVAN to s3
    Returns nothing
    """
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at bhuvan_upload_to_s3")


@flow
def ids_drr_flow(pipeline):
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
