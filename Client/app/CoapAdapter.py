from aiocoap import *
import threading
import asyncio


class CoapAdapter:

    def __init__(self, controller):
        self.controller = controller
        self.broker_address = None
        self.protocol = None

    async def connect(self, address):
        self.broker_address = address

    async def subscribe(self, topic, qos=None):
        new_loop = asyncio.new_event_loop()
        t = threading.Thread(target=self.start_loop, args=(new_loop, topic, ))
        t.start()

    async def publish(self, topic, identifier, payload, settings):
        msg = Message(code=PUT, uri="coap://[{0}]:5683/ps/{1}".format(self.broker_address, topic),
                      payload=bytes(str(identifier + payload), encoding="utf-8"))
        response = await self.protocol.request(msg).response

    async def start_client(self):
        self.protocol = await Context.create_client_context()

    async def stop_client(self):
        pass

    def start_loop(self, loop, topic):
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.observing(topic))

    async def observing(self, topic):
        protocol = await Context.create_client_context()
        # Address for Test Broker: 2402:9400:1000:7::FFFF
        msg = Message(code=POST, uri="coap://[{0}]:5683/ps".format(self.broker_address), payload=bytes("<{0}>;ct=0;".format(topic), encoding="utf-8"))
        response = await protocol.request(msg).response

        request = Message(code=GET, uri='coap://[{0}]:5683/ps/{1}'.format(self.broker_address, topic), observe=0)
        pr = protocol.request(request)
        r = await pr.response

        async for r in pr.observation:
            self.controller.react(topic, r.payload.decode("utf-8"))
