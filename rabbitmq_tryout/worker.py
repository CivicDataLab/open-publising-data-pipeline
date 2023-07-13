@task
def mgnrega_scraper(context, pipeline, task_obj):
    data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)
    if not exception_flag:
        # if there's no error while executing the task
        # Replace the following with your own code if the need is different.
        # Generally, to read the returned data into a dataframe and save it against the pipeline object for further tasks
        print(data)
    else:
            pipeline.logger.error("ERROR: at mgnrega_scraper")