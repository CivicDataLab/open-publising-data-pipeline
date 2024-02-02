from prefect import task, flow
from task_utils import *


# {
#     "pipeline_name": "pipe14",
#     "data_url": "",
#     "project": "ids-drr",
#     "transformers_list": [
#         {
#             "name": "tenders_scrape_data",
#             "order_no": 1,
#             "context": {
#                 "year": "2022",
#                 "month": "4"
#             }
#         },
#         {
#             "name": "tenders_scrape_data",
#             "order_no": 1,
#             "context": {
#
#             }
#         }
#     ]
# }
# {
#     "pipeline_name": "pipe14",
#     "data_url": "",
#     "project": "ids-drr",
#     "transformers_list": [
#         {
#             "name": "tenders_concat_tenders",
#             "order_no": 1,
#             "context": {
#
#             }
#         }
#     ]
# }


# ,
#         {
#             "name": "tenders_geocode_district",
#             "order_no": 2,
#             "context": {}
#         },
#         {
#             "name": "tenders_geocode_rc",
#             "order_no": 3,
#             "context": {}
#         },
#         {
#             "name": "tenders_transformer",
#             "order_no": 4,
#             "context": {}
#         }
@task
def prepare_master_variable_csv(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:    # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        #df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
       pipeline.logger.error("ERROR: at prepare_master_variable_csv")
@task
def prepare_master_file(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data_path)
    if not exception_flag:    # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        #df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
       pipeline.logger.error("ERROR: at prepare_master_file")

@task
def tenders_transformer(context, pipeline, task_obj):
    """
    Input - Nothing
    Output - Returns nothing
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_transformer")


@task
def tenders_geocode_rc(context, pipeline, task_obj):
    """
    Input - Nothing
    Output - Returns nothing
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_geocode_rc")


@task
def tenders_geocode_district(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_geocode_district")


@task
def tenders_get_flood_tenders(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_get_flood_tenders")


@task
def tenders_concat_tenders(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_concat_tenders")


@task
def tenders_scrape_data(context, pipeline, task_obj):
    """
    Input - year and month in YYYY and M format
    example context - {
        "year": "2022",
        "month": "4"
    }

    Output: Creates folders in the server. Returns nothing.
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        # df = pd.read_csv(StringIO(data), sep=',')
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at tenders_scrape_data")


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
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at collect_imd_data")


@task
def sentinel_upload_to_s3(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at sentinel_upload_to_s3")


@task
def sentinel(context, pipeline, task_obj):
    """
    Input - date_from in yyyy-MM-dd format
            date_to in yyyy-MM-dd format
            state_name in str format
    context would look like the following
    {
        "date_from": "2023-01-01",
        "date_to": "2023-06-01",  
        "state_name": "assam"
    }

    Output - The task creates files and folders in the server. Nothing is returned.
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at sentinel")


@task
def gcn250(context, pipeline, task_obj):
    """
    Input - state_name in str format
    context would look like the following
    {
        "state_name": "assam"
    }

    Output - The task creates files and folders in the server. Nothing is returned.
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
    if not exception_flag:  # if there's no error while executing the task
        pipeline.data_path = data
        # Following is a mandatory line to set logs in prefect UI
        set_task_model_values(task_obj, pipeline)
    else:
        pipeline.logger.error("ERROR: at gcn250")


@task
def bhuvan_get_dates(context, pipeline, task_obj):
    """
    Input - No input
    Output - Dates the images are available on BHUVAN
    """
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
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

    context = context.update(
        {"dates": pipeline.data_path}
    )  # update the context with dates got from - get_dates
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
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
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
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
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
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
    data, exception_flag = publish_task_and_process_result(
        task_obj, context, pipeline.data_path
    )
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
            pipeline.logger.info(
                f"""INFO: The task - {task.task_name} was failed. Set Pipeline status to failed."""
            )
            pipeline.model.save()
            break
    if pipeline.model.status != "Failed":
        pipeline.model.status = "Done"
        pipeline.logger.info(f"""INFO: Set Pipeline status to Done.""")
        pipeline.model.save()
    pipeline.model.output_id = (
        str(pipeline.model.pipeline_id) + "_" + pipeline.model.status
    )
    # print("Data after pipeline execution\n", pipeline.data)
    return
