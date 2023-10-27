import json
import os
import uuid

import pandas as pd
import pika

# from tasks.scripts.s3_utils import upload_result

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

channel = connection.channel()
channel.exchange_declare(exchange="topic_logs", exchange_type="topic")
result = channel.queue_declare("", exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_key = "merge_columns"

channel.queue_bind(exchange="topic_logs", queue=queue_name, routing_key=binding_key)


def merge_columns(context, data_path):
    try:
        cols_to_merge = context["columns"]  # list
        print(cols_to_merge)
        output_column = context["output_column"]
        print(output_column)
        separator = context["separator"]
        drop_flag = context["drop_flag"]
        transformed_data = pd.read_csv(data_path)
        transformed_data[output_column] = (
            transformed_data[cols_to_merge]
            .astype(str)
            .agg(f"""{separator}""".join, axis=1)
        )
        if drop_flag == "True":
            transformed_data = transformed_data.drop(cols_to_merge, axis=1)
        temp_file_name = str(uuid.uuid4())
        is_exists = os.path.exists("pipeline_temp_files")
        if not is_exists:
            os.mkdir("pipeline_temp_files")
        file_name = "pipeline_temp_files/" + temp_file_name + ".csv"
        transformed_data.to_csv(file_name, index=False)
        data_file_path = os.path.abspath(file_name)
    except Exception as e:
        return "Worker failed with an error - " + str(e)
    # return the transformed data
    return data_file_path


def on_request(ch, method, props, body):
    # send the worker-alive message if the request message is -> get-ack
    if body.decode("utf-8") == "get-ack":
        print("inside if..")
        ch.basic_publish(
            exchange="",
            routing_key=props.reply_to,
            properties=pika.BasicProperties(
                correlation_id=props.correlation_id, delivery_mode=2
            ),
            body="worker alive",
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        # if the message is other than "get-ack" then carryout the task
        task_details = json.loads(body)
        context = task_details["context"]
        data_path = task_details["data_path"]
        try:
            response = merge_columns(context, data_path)
            # with open("merge_col_result", "wb") as f:
            #     f.write(str(response_msg.text))
            #     s3_link = upload_result("merge_col_result")
            ch.basic_publish(
                exchange="",
                routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id, delivery_mode=2
                ),
                body=str(response),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print("[x] sent the response to the client..", response)
        except Exception as e:
            raise e


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")

channel.start_consuming()
