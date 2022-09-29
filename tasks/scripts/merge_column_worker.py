import json

import pandas as pd
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=False, durable=True)
queue_name = result.method.queue

binding_key = "merge_columns"
channel.queue_bind(exchange='topic_logs', queue=queue_name, routing_key=binding_key)

def merge_columns(context, data):
    column1, column2, output_column = context['column1'], context['column2'], context['output_column']
    separator = context['separator']
    transformed_data = pd.read_json(data)
    try:
        print("inside merge columns..")
        transformed_data[output_column] = transformed_data[column1].astype(str) + separator + transformed_data[column2].astype(str)
        transformed_data = transformed_data.drop([column1, column2], axis=1)
    except Exception as e:
        print(e)
    return transformed_data


def on_request(ch, method, props, body):
    task_details = json.loads(body)
    context = task_details["context"]
    data = task_details["data"]
    print("body",context)
    print("hereeee")
    print(type(context))
    response = merge_columns(context, data)
    response_msg = response.to_csv()
    print("response in worker...", response)
    ch.basic_publish(exchange="",
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id,delivery_mode=2),
                     body=str(response_msg))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=2)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()