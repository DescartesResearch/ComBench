from fastapi import FastAPI
from hypercorn.config import Config
from hypercorn.asyncio import serve
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel
from typing import List
from typing import Tuple
from typing import Optional
from starlette.responses import FileResponse
import Controller


app = FastAPI()
logging.root.handlers = []
logging.basicConfig(filename="info.log")
basic_logger = logging.getLogger("ClientLogger")
basic_logger.addHandler(logging.FileHandler("info.log"))
basic_logger.setLevel(logging.INFO)
basic_logger.info("client started")
controller = Controller.Controller()


class TimedVariable(BaseModel):
    type: str
    delay: int


class Trigger(BaseModel):
    type: str
    subscription: str = None
    timed_variable: TimedVariable


class Publishing(BaseModel):
    topic: str
    payload_size: int
    trigger: Trigger


class Subscription(BaseModel):
    topic: str


class Role(BaseModel):
    id: int
    subscriptions: List[Subscription]
    publishings: List[Publishing]


class QualityClass(BaseModel):
    id: int
    packet_loss: float = None
    bandwidth: str = None
    delay: int = None
    # [runtime in seconds, packet-loss, bandwidth, delay in ms]
    time_series: List[Tuple[Optional[int], Optional[float], Optional[str], Optional[int]]] = None


class Settings(BaseModel):
    qos: int
    tls: bool


class Client(BaseModel):
    protocol: str
    start_time: float
    run_time: int
    broker_address: str
    role: Role
    quality_class: QualityClass = None
    settings: Settings = None
    name: str


@app.get('/')
def hello_world():
    return 'Hello World!'


@app.put('/startClient')
async def start_client(client: Client):
    logging.info("configuration received")
    global controller
    controller = Controller.Controller()

    protocol = client.protocol
    broker_address = client.broker_address
    start_time = datetime.fromtimestamp(client.start_time, tz=timezone.utc)

    run_time = client.run_time

    quality_class = client.quality_class
    name = client.name
    settings = client.settings

    role = client.role
    controller.create_components(name, start_time, run_time, broker_address, protocol, settings)

    for subscription in role.subscriptions:
        await controller.manage_subscription(subscription.topic, settings)

    await controller.start_client()

    for publishing in role.publishings:
        topic = publishing.topic
        payload_size = publishing.payload_size
        trigger_type = publishing.trigger.type
        subscription = None
        if trigger_type == "reaction":
            subscription = publishing.trigger.subscription
            if subscription is None:
                raise Exception("A subscription is needed for a reaction trigger!")

        timed_variable = publishing.trigger.timed_variable.type
        value = publishing.trigger.timed_variable.delay
        controller.manage_publishing(topic, payload_size, trigger_type, subscription, timed_variable, value, settings)

    ##TODO: Reihenfolge 1.subscriben 2. netzwerk 3. measurement mit richtigem netzwerk device
    await controller.configure(protocol, broker_address, start_time, run_time, quality_class, name, settings)

    return client


@app.get("/results")
def send_results():
    return FileResponse(Path().absolute().joinpath("{0}.csv".format(controller.name)))


@app.get("/resourceResults")
def send_resource_measurements():
    return FileResponse(Path().absolute().joinpath("{0}resources.csv".format(controller.name)))


@app.get("/networkResults")
def send_network_measurements():
    return FileResponse(Path().absolute().joinpath("{0}network.csv".format(controller.name)))


@app.get("/warningLogs")
def send_warning_logs():
    return FileResponse(Path().absolute().joinpath("{0}warning.log".format(controller.name)))


if __name__ == '__main__':
    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(serve(app, config))
