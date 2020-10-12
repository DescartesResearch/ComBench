# import paho.mqtt.client as mqtt
import asyncio_mqtt as amqtt
import threading
import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
import ssl


class MqttAdapter:

    def __init__(self, name, controller):
        self.controller = controller
        self.name = name
        self.client: amqtt.Client
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        #self.ssl_context = ssl.create_default_context()
        #self.client.tls_set_context(self.ssl_context)

    async def connect(self, address, settings):
        if settings.tls:
            self.client = amqtt.Client(address, client_id=self.name, port=8883)
            self.client._client.tls_set(cert_reqs=ssl.CERT_NONE)
            self.client._client.tls_insecure_set(True)
        else:
            self.client = amqtt.Client(address, client_id=self.name)
        await self.client.connect()

    async def subscribe(self, topic, settings):
        qos = None
        if settings is not None:
            qos = settings.qos
        print("subscribed to " + topic)
        if qos is None:
            await self.client.subscribe(topic)
        else:
            await self.client.subscribe(topic, qos=qos)
        asyncio.ensure_future(self.observing(topic))

    async def publish(self, topic, identifier, payload, settings, retain=False):
        qos = None
        if settings is not None:
            qos = settings.qos
        await self.client.publish(topic, str(identifier + payload), qos, retain)

    async def start_client(self):
        pass

    async def listen(self, messages):
        async for message in messages:
            self.callback("test", "test", message)

    async def stop_client(self):
        print("disconnecting client")
        await self.client.disconnect()

    def callback(self, client, user_data, message):
        self.controller.react(message.topic, message.payload.decode("utf-8"))

    def start_loop(self, topic):
        self.loop.run_until_complete(self.observing(topic))

    async def observing(self, topic):
        async with self.client.filtered_messages(topic) as messages:
            async for message in messages:
                self.callback("test", "test", message)
