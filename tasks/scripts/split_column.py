import json

import pandas as pd
import pika

# from tasks.scripts.s3_utils import upload_result

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

print("queue name----", queue_name)
binding_key = "split_column"

channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)


def split_columns(context, data):
    try:
        column = context['column']
        new_col_names = context['new_cols'] #list
        print(type(new_col_names))
        sep = context['sep']
        data = pd.read_json(data)
        # print(data)
        print(type(data))
        print(data[column], "ERRENOUS")
        data[new_col_names] = pd.Series(data[column]).str.split(sep, expand=True)
    except Exception as e:
        return "Worker failed with an error - " + str(e)
    # return the transformed data
    return data


def on_request(ch, method, props, body):
    # send the worker-alive message if the request message is -> get-ack
    if body.decode('utf-8') == 'get-ack':
        print("inside if..")
        ch.basic_publish(exchange="",
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id, delivery_mode=2),
                         body='worker alive'.encode("utf-8"))
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        # if the message is other than "get-ack" then carryout the task
        task_details = json.loads(body)
        context = task_details["context"]
        data = task_details["data"]
        try:
            response = split_columns(context, data)
            if isinstance(response, pd.core.frame.DataFrame):
                response_msg = response.to_csv()
            else:
                response_msg = response
            # with open("xyz", "wb") as f:
            #     f.write(str(response_msg.text))
            #     s3_link = upload_result("xyz")
            ch.basic_publish(exchange="",
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id, delivery_mode=2),
                             body=str(response_msg))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print("[x] sent the response to the client..")
        except Exception as e:
            raise e


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")

channel.start_consuming()