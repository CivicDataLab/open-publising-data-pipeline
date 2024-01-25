import json

import pandas as pd
import pika


class Worker:
    """
    The Worker class can be used as a wrapper around the independently written tasks(methods.)
    """
    def __init__(self, task_name):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost', heartbeat=0))

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
        self.result = self.channel.queue_declare('', exclusive=False, durable=True)
        self.queue_name = self.result.method.queue

        print("queue name----", self.queue_name)
        self.binding_key = task_name

        self.channel.queue_bind(exchange='topic_logs', queue=self.queue_name, routing_key=self.binding_key)
        self.dynamic_method = None

    def add_task(self, method):
        """
        Use this method to add the defined task.
        """
        self.dynamic_method = method

    def execute_task(self, context, data_path):
        """
        This is called internally to execute the task that is added to the class.
        """
        try:
            method = self.dynamic_method
            return method(context, data_path)
        except Exception as e:
            return "Worker failed with an error - " + str(e)

    def on_request(self, ch, method, props, body):
        # send the worker-alive message if the request message is -> get-ack
        if body.decode('utf-8') == 'get-ack':
            print("inside if..")
            ch.basic_publish(exchange="",
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id, delivery_mode=2),
                             body='worker alive')
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # if the message is other than "get-ack" then carryout the task
            task_details = json.loads(body)
            context = task_details["context"]
            data_path = task_details["data_path"]
            try:
                response = self.execute_task(context, data_path)
                if isinstance(response, pd.core.frame.DataFrame):
                    response_msg = response.to_csv()
                else:
                    response_msg = response
                # with open("{output_file_name}", "wb") as f:
                #     f.write(str(response_msg.text))
                #     s3_link = upload_result("{output_file_name}")
                ch.basic_publish(exchange="",
                                 routing_key=props.reply_to,
                                 properties=pika.BasicProperties(correlation_id=props.correlation_id, delivery_mode=2),
                                 body=str(response_msg))
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print("[x] sent the response to the client..")
            except Exception as e:
                raise e

    def start_worker(self):
        """
        Calling this method would start the worker.
        """
        self.channel.basic_qos(prefetch_count=2)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_request)

        print(" [x] Awaiting RPC requests")

        self.channel.start_consuming()
