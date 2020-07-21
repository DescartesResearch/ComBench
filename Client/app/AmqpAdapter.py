import pika
import threading


class AmqpAdapter:

    def __init__(self, name, controller):
        self.sub_connection = None
        self.sub_channel = None
        self.pub_connection = None
        self.pub_channel = None
        self.name = name
        self.controller = controller
        self.properties = {}

    async def connect(self, address):
        self.sub_connection = pika.BlockingConnection(pika.ConnectionParameters(address))
        self.sub_channel = self.sub_connection.channel()
        self.pub_connection = pika.BlockingConnection(pika.ConnectionParameters(address))
        self.pub_channel = self.pub_connection.channel()

    async def subscribe(self, topic, qos=None):
        self.sub_channel.queue_declare(topic)
        self.sub_channel.basic_consume(queue=topic, auto_ack=True, on_message_callback=self.callback)
        print("subscribed to " + topic)

    async def publish(self, topic, identifier, payload, settings):
        prop = self.properties.get(topic)
        if prop is None:
            self.properties[topic] = pika.BasicProperties(reply_to=topic)
            prop = self.properties[topic]
        self.pub_channel.basic_publish(exchange='', routing_key=topic, body=str(identifier + payload), properties=prop)

    async def start_client(self):
        thread = threading.Thread(target=self.looping)
        thread.start()

    async def stop_client(self):
        try:
            self.sub_channel.stop_consuming()
            self.sub_connection.close()
            self.pub_connection.close()
        except:
            print("Client disconnected")

# Topicname wird aus Nachrichteninhalt übergeben
    def callback(self, ch, method, properties, body):
        topic = properties.reply_to
        self.controller.react(topic, body.decode("utf-8"))

    def looping(self):
        try:
            self.sub_channel.start_consuming()
        except:
            print("Connection closed")
