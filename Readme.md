# Project Overview

This Django project is designed to create and manage data pipelines. The project comprises two primary components:

1.  **Task Publisher**: A [Prefect](https://docs.prefect.io/) pipeline responsible for publishing tasks in the order they are received from API requests via the user interface.

2.  **Task Executor**: A daemon process that executes tasks when they are published for it. These are the tasks that are deployed on different servers.  

To implement a 'publish-subscribe pattern,' [RabbitMQ](https://www.rabbitmq.com/documentation.html) is utilized.

# Server Setup

To set up the server, please refer to the guide provided in [this blog](https://medium.com/civicdatalab/data-orchestration-using-prefect-and-rabbitmq-6349096daca5).
Currently, the API requests are sent to `node-1` server that we are using as a publisher. 

# RabbitMQ Configuration

RabbitMQ requires specific configuration for long-running tasks. Follow these steps:

1.  Add the following line to the RabbitMQ configuration file: `/etc/rabbitmq/rabbitmq.conf` (create the file if it doesn't exist).

`consumer_timeout = 10800000`

The above line sets the consumer timeout to be 3 hours (in milliseconds).

2.  Ensure the RabbitMQ server can access the configuration file by granting the necessary permissions (e.g., `chmod 777`).

3.  After saving the configuration file, restart the RabbitMQ server using the following command:

`sudo systemctl restart rabbitmq-server`

4.  If the restart command takes too long to execute, you can stop the execution and force boot the RabbitMQ service with the following command:

`sudo rabbitmqctl force_boot`

After this, RabbitMQ server can be restarted.

The data pipelines should now be ready to operate efficiently.

The data pipelines should now be ready to operate efficiently.

## Project Flow

![Flow chart](flow_diagram.png)

## Background tasks

To start the server to accept the API requests, run the following command.\
`python manage.py runserver` - This starts the Django server, and listens to the API requests. This can be considered an entry-point to our program. 

The following command needs to be run to push the API calls to queue. 

2.  `python manage.py runscript worker_demon.py` - Runs the rabbitmq - worker demon. This is responsible for translating API calls to models and publishing each task in the corresponding queue.  

## Sample API Request

A demo request to the shepherd API consists the following in the request body. 

    {
     "pipeline_name": "sentinel_pipeline",
     "data_url": "",
     "project": "ids-drr",
      "transformers_list" : [
           {
              "name": "sentinel",
              "order_no": 1, 
              "context": {
                  "date_start": "2023-05-01",
                  "date_end": "2023-06-01"
              }
           },
           {
              "name": "sentinel_upload_to_s3",
              "order_no": 2, 
              "context": {}
           }
        ]
    }

1.  *pipeline_name* - Name of the pipeline. This can be anything. 
2.  *data_url* - URL to the data file. Some pipelines don't need data file as an input. In that case keep this field empty. 
3.  *project* - Name of the project that the pipeline belongs to. Refer to [model_to_pipeline](pipeline/model_to_pipeline.py) for valid project names. 
4.  *transformers_list* - List of json objects that contains the details of the tasks.
    1.  *name* - Name of the task that needs to be performed. Please note that this name should be same as the method defined in the project flow.
    2.  *order_no* - The order number of the task. In the above example task - *sentinel* is followed by the task - *sentinel_upload_to_s3* owing to their order numbers.
    3.  *context* - Necessary inputs to perform the task. This is task specific. In the above example, the task - *sentinel* requires start and end dates as inputs.    

## Adding Publisher to the pipeline

Following are the steps to be followed to add a new task to the pipeline. 

1.  Define your task name and the context (i.e. necessary information to perform the task).
2.  The name of the publisher method, and the name of the task should be same. This is the name that we use in the *transformers_list* of API call.   
3.  Decide the project that you want to add the task in. Projects can be found in the [projects](projects) folder. 
4.  Generate the publisher code by running `python -m code_templates.publisher_template --task_name ''` in a terminal.
5.  Paste the generated code in the Python file that contains the project flow. 
    The following line publishes the task along with the context and the data in the queue.
6.  `data, exception_flag = publish_task_and_process_result(task_obj, context, pipeline.data)  
     if not exception_flag:`
7.  Once the task is published and the worker finishes the task, the value returned by the worker will be stored in the variable named - `data` in the above line. Note that this data can be anything, say - a pandas dataframe, a path string etc. This will be saved in the pipeline object so that the next task can pick this up if needed. So, it is up to the task creator what to return.

# Writing the worker

1.  Worker is any piece of code that has a RabbitMQ wrapper around it so that if publisher publishes a task, it can pick the task up and execute the code. 
2.  To create a worker generate the boilerplate by running the following command. 
    `python -m code_templates.task_template.py --task_name "" --result_file ""`
    The above command will generate a template, in which the task specific logic can be written. Go through the template, and there will be a method defined with the task name and the logic needs to be written within the method. Any other method can be called from within this method so that the task remains modular. Make sure that everything is written within try block so that if anything goes wrong inside the worker it responds back to the publisher.  
3.  Save the file with `.py` extension and deploy it onto any server that is clustered with `node-1 server`. 
4.  Once the worker code is deployed, it can be run with `python filename.py` - this will start the worker. Whenever the publisher publishes a message for this worker, worker picks the message, processes it and returns the response back to the publisher.

# API - documentation for the existing projects

The pipeline currently supports the following projects. 

1.  IDS-DRR
2.  Data for Districts

### IDS-DRR Sources and API calls to trigger them

-   #### BHUVAN
    BHUVAN has 4 tasks defined. The following API call is used to trigger those tasks one after the other. 

```commandLine
POST /transformer/pipe_create HTTP/1.1
Host: 43.205.116.57:8000
Content-Type: application/json
Content-Length: 680

{
    "pipeline_name": "Run BHUVAN source",
    "data_url": "",
    "project": "ids-drr",
    "transformers_list": [
        {
            "name": "bhuvan_get_dates",
            "order_no": 1,
            "context": {}
        },
        {
            "name": "bhuvan_gdal_wms",
            "order_no": 2,
            "context": {}
        },
        {
            "name": "bhuvan_remove_watermark",
            "order_no": 3,
            "context": {}
        },
        {
            "name": "bhuvan_transformer",
            "order_no": 4,
            "context": {
                "year": "2023",
                "month": "06"
            }
        }
    ]
}
```

-   #### SENTINEL

```commandline
POST /transformer/pipe_create HTTP/1.1
Host: 43.205.116.57:8000
Content-Type: application/json
Content-Length: 454

{
    "pipeline_name": "sentinel_pipeline",
    "data_url": "",
    "project": "ids-drr",
    "transformers_list": [
        {
            "name": "sentinel",
            "order_no": 1,
            "context": {
                "date_start": "2023-05-01",
                "date_end": "2023-06-01"
            }
        },
        {
            "name": "sentinel_upload_to_s3",
            "order_no": 2,
            "context": {}
        }
    ]
}
```

-   #### IMD

```commandline
POST /transformer/pipe_create HTTP/1.1
Host: 43.205.116.57:8000
Content-Type: application/json
Content-Length: 283

{
    "pipeline_name": "IMD pipeline",
    "data_url": "",
    "project": "ids-drr",
    "transformers_list": [
        {
            "name": "collect_imd_data",
            "order_no": 1,
            "context": {
                "year": "2022"
            }
        }
    ]
}
```

### Data for Districts tasks

--To be added--