import json as Json
import time
import os
from os import listdir
import shutil
import zipfile
from datetime import datetime
from datetime import timedelta
import requests

from fastapi import FastAPI
import uvicorn
from hypercorn.config import Config
from hypercorn.asyncio import serve
import asyncio
from pydantic import BaseModel
from typing import List
from typing import Tuple
from typing import Optional
from typing import Union
from starlette.responses import FileResponse

from pathlib import Path
import pandas
import matplotlib.pyplot as plt
import math
import csv

app = FastAPI()
client_info = []
client_names = []
run_info = []
topic_receivers = {}
dirs = {}


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
    qos: int = None
    tls: bool = False


class Client(BaseModel):
    ip_address: str
    begin_port: int
    role: int
    quality_class: int
    amount: int
    settings: Settings = None


class Configuration(BaseModel):
    protocol: str
    delay: int = None
    start_time: str = None
    run_time: int
    broker_address: str
    quality_classes: List[QualityClass] = None
    roles: List[Role]
    clients: List[Client]


@app.get('/')
def hello_world():
    return 'Hello World!'


@app.put('/startBenchmark')
def start_benchmark(configuration: Configuration):
    client_info.clear()
    run_info.clear()
    topic_receivers.clear()

    protocol = configuration.protocol

    # start time in iso 8601
    if configuration.start_time is not None:
        start_time = configuration.start_time
        start_time = datetime.strptime(start_time, '%Y-%m-%dT%H:%M:%S%z').timestamp() + configuration.delay
    else:
        start_time = time.time() + configuration.delay
        # start_time = datetime.fromtimestamp(int(time.time()) + configuration.delay).isoformat()

    run_time = configuration.run_time

    # save run and start time
    run_info.append(run_time)
    run_info.append(start_time)

    broker_address = configuration.broker_address

    quality_classes = configuration.quality_classes

    roles = configuration.roles

    clients = configuration.clients

    json_data = {"protocol": protocol, "start_time": start_time, "run_time": run_time, "broker_address": broker_address}

    addresses = []

    for client in clients:
        json_data["role"] = roles[int(client.role)]
        class_index = client.quality_class
        if class_index is not None:
            json_data["quality_class"] = quality_classes[class_index]
        if client.settings is not None:
            json_data["settings"] = client.settings

        for subscription in json_data["role"].subscriptions:
            if topic_receivers.get(subscription.topic) is None:
                topic_receivers[subscription.topic] = client.amount
            else:
                topic_receivers[subscription.topic] += client.amount

        address = client.ip_address
        begin_port = client.begin_port

        client_info.append([client.amount, address, begin_port])

        for i in range(0, client.amount):
            if address not in addresses and class_index is not None:
                addresses.append(address)
            else:
                pass
                # json_data["quality_class"] = None
            json_data["name"] = "Client" + address + ":" + str(begin_port + i)
            client_names.append("Client" + address + ":" + str(begin_port + i))
            print(json_data)
            r = requests.put("http://{0}:{1}/startClient".format(address, begin_port + i), data=Json.dumps(json_data, default=lambda x: x.__dict__))

    run_info.append(configuration.json(indent=2))

    return "Benchmark run started"


@app.get("/collectResults")
def collect_results():
    run_count = 0
    path_to_dir = Path().absolute().parent.joinpath(Path("benchmark_run_{0}".format(run_count)))
    while os.path.exists(path_to_dir):
        run_count += 1
        path_to_dir = Path().absolute().parent.joinpath(Path("benchmark_run_{0}".format(run_count)))
    result_dir = path_to_dir.joinpath("results")
    raw_dir = path_to_dir.joinpath("raw_data")

    os.makedirs(result_dir)
    os.makedirs(raw_dir)

    dirs["run_dir"] = str(path_to_dir)
    dirs["result_dir"] = str(result_dir)
    dirs["raw_dir"] = str(raw_dir)

    with open(dirs["result_dir"] + "/configuration.json", "w") as file:
        file.write(run_info[-1])

    j = 0
    for client in client_info:
        for i in range(0, client[0]):
            r = requests.get("http://{0}:{1}/results".format(client[1], client[2] + i))
            file = open(dirs["raw_dir"] + '/' + client_names[j] + '.csv', 'w')
            file.truncate(0)
            file.close()
            with open(dirs["raw_dir"] + '/' + client_names[j] + '.csv', mode='wb') as file:
                file.write(r.content)

            r = requests.get("http://{0}:{1}/resourceResults".format(client[1], client[2] + i))
            file = open(dirs["raw_dir"] + '/' + client_names[j] + '-resources.csv', 'w')
            file.truncate(0)
            file.close()
            with open(dirs["raw_dir"] + '/' + client_names[j] + '-resources.csv', mode='wb') as file:
                file.write(r.content)

            r = requests.get("http://{0}:{1}/networkResults".format(client[1], client[2] + i))
            file = open(dirs["raw_dir"] + '/' + client_names[j] + '-network.csv', 'w')
            file.truncate(0)
            file.close()
            with open(dirs["raw_dir"] + '/' + client_names[j] + '-network.csv', mode='wb') as file:
                file.write(r.content)

            r = requests.get("http://{0}:{1}/warningLogs".format(client[1], client[2] + i))
            file = open(dirs["raw_dir"] + '/' + client_names[j] + '-warning.log', 'w')
            file.truncate(0)
            file.close()
            with open(dirs["raw_dir"] + '/' + client_names[j] + '-warning.log', mode='wb') as file:
                file.write(r.content)

            j += 1
    return "Results Collected"


@app.get("/evaluate")
def evaluate():

    number_of_clients = 0
    for client in client_info:
        number_of_clients += client[0]

    result_dir = dirs["result_dir"]

    with open(result_dir + "/Summary.txt", mode="w") as file:
        s = "##### Resource Measurement Results ######\n\n"
        file.write(s)

    send_dataframe = pandas.DataFrame({'type': [],
                                       'uuid': [],
                                       'timestamp': [],
                                       'message': []})

    for i in range(number_of_clients):
        send_dataframe = send_dataframe.append(pandas.read_csv(dirs["raw_dir"] + '/' + client_names[i] + '.csv',
                                                               names=["type", "uuid", "timestamp", "message"], header=None,
                                                               dtype={"type": str, "uuid": str, "timestamp": int, "message": str}))


    rec_dataframe = send_dataframe.loc[send_dataframe["type"] == "r"]
    send_dataframe = send_dataframe.loc[send_dataframe["type"] == "s"]

    rec_dataframe["runtime"] = (rec_dataframe["timestamp"] - run_info[1]*10**9) / 10**9
    send_dataframe["runtime"] = (send_dataframe["timestamp"] - run_info[1]*10**9) / 10**9

    max_number_of_cores = 0
    for i in range(number_of_clients):
        f = open(dirs["raw_dir"] + "/" + client_names[i] + "-resources.csv", 'r')
        reader = csv.reader(f, delimiter=",")
        columns = len(next(reader))
        if max_number_of_cores < columns - 3:
            max_number_of_cores = columns - 3

    csv_row = ["client_name"]
    for i in range(max_number_of_cores):
        csv_row.append("cpu_total_mean_core_" + str(i))
        csv_row.append("cpu_total_sd_core_" + str(i))
    csv_row.extend(["cpu_process_mean", "cpu_process_sd", "ram_total_mean", "ram_total_sd", "ram_process_mean",
                    "ram_process_sd", "number_of_rec_messages", "number_of_sent_messages", "avg_of_rec_messages",
                    "avg_of_sent_messages", "latency_total_mean", "latency_total_median", "latency_total_min",
                    "latency_total_max", "latency_total_sd", "latency_sent_mean", "latency_sent_median",
                    "latency_sent_min", "latency_sent_max", "latency_sent_sd", "latency_rec_mean", "latency_rec_median",
                    "latency_rec_min", "latency_rec_max", "latency_rec_sd", "total_losses", "losses_in_percent"])
    with open(result_dir + "/Summary.csv", mode="w") as file:
        csv_writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(csv_row)

    for i in range(number_of_clients):
        client_s_df = pandas.read_csv(dirs["raw_dir"] + '/' + client_names[i] + '.csv',
                        names=["type", "uuid", "timestamp", "message"], header=None,
                        dtype={"type": str, "uuid": str, "timestamp": int, "message": str})

        client_r_df = client_s_df.loc[client_s_df["type"] == "r"]
        client_s_df = client_s_df.loc[client_s_df["type"] == "s"]

        client_r_df["runtime"] = (client_r_df["timestamp"] - run_info[1] * 10 ** 9) / 10**9
        client_s_df["runtime"] = (client_s_df["timestamp"] - run_info[1] * 10 ** 9) / 10**9

        client_rec_messages = len(client_r_df.index)
        client_sent_messages = len(client_s_df.index)

        avg_sent_messages = client_sent_messages / ((run_info[0]) / 60)
        avg_rec_messages = client_rec_messages / ((run_info[0]) / 60)

        client_s_df = client_s_df.sort_values(by="timestamp")
        client_r_df = client_r_df.sort_values(by="timestamp")
        fig, ax = plt.subplots()
        ax.plot(client_s_df["runtime"], range(1, client_sent_messages + 1), linewidth=0.5, label="sent messages")
        ax.plot(client_r_df["runtime"], range(1, client_rec_messages + 1), linewidth=0.5, label="received messages")
        ax.set_xlabel("seconds")
        ax.set_ylabel("number of messages")
        ax.set_title("Received and sent messages " + client_names[i])
        plt.legend()
        fig.savefig(result_dir + "/messages-" + client_names[i] + ".pdf")
        plt.close(fig)

        client_s_latency_df = pandas.merge(client_s_df, rec_dataframe, on="uuid", how="inner")
        client_r_latency_df = pandas.merge(send_dataframe, client_r_df, on="uuid", how="inner")
        client_latency_df = pandas.concat([client_r_latency_df, client_s_latency_df])

        client_latency_mean = []
        client_latency_median = []
        client_latency_min = []
        client_latency_max = []
        client_latency_sd = []

        for df, name, color in zip([client_latency_df, client_s_latency_df, client_r_latency_df], ["combined", "sent", "rec"], ["blue", "blue", "red"]):
            df["latency"] = (df["timestamp_y"] - df["timestamp_x"]) / 10**6
            df = df.sort_values(by="timestamp_x")
            if name != "combined":
                plot = df.plot.scatter(y='latency', x='runtime_x', s=2, c=color)
                plot.set_xlabel("time (sent) in s")
                plot.set_ylabel("latency in milliseconds")
                plot.set_title("Latency from " + client_names[i] + " " + name + " messages")
                fig = plot.get_figure()
                fig.savefig(result_dir + "/latency-" + client_names[i] + "-" + name + ".pdf")
                plt.close(fig)

            df.to_csv(result_dir + "/latency-" + client_names[i] + "-" + name + ".csv", columns=["runtime_x", "latency"], index=False)

            client_latency_mean.append(str(df["latency"].mean()))
            client_latency_median.append(str(df["latency"].median()))
            client_latency_min.append(str(df["latency"].min()))
            client_latency_max.append(str(df["latency"].max()))
            client_latency_sd.append(str(math.sqrt(df.loc[:, "latency"].var())))

        fig, ax = plt.subplots()
        ax.scatter(client_s_latency_df["runtime_x"], client_s_latency_df["latency"], s=2, label="sent messages", c="blue")
        ax.scatter(client_r_latency_df["runtime_x"], client_r_latency_df["latency"], s=2, label="rec messages", c="red")
        ax.set_xlabel("time (sent) in s")
        ax.set_ylabel("latency in milliseconds")
        ax.set_title("Received and sent messages " + client_names[i])
        plt.legend()
        fig.savefig(result_dir + "/latency-" + client_names[i] + "-combined.pdf")
        plt.close(fig)

        f = open(dirs["raw_dir"] + "/" + client_names[i] + "-resources.csv", 'r')
        reader = csv.reader(f, delimiter=",")
        columns = len(next(reader))
        del reader
        del f
        my_cols = []
        for j in range(columns - 3):
            my_cols.append("cpu_total_core{0}".format(j))
        my_cols.append("cpu_of_process")
        my_cols.append("ram_total")
        my_cols.append("ram_of_process")

        dtype_dict = {}
        for j in range(len(my_cols) - 1):
            dtype_dict[my_cols[j]] = float
        dtype_dict["ram_of_process"] = int

        resource_df = pandas.read_csv(dirs["raw_dir"] + "/" + client_names[i] + "-resources.csv", names=my_cols, dtype=dtype_dict, header=None)

        plot = resource_df.plot(y=my_cols[:-3], kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("CPU load in percent")
        plot.set_title("Total CPU load per core from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/CPU_total-" + client_names[i] + ".pdf")
        plt.close(fig)

        plot = resource_df.plot(y="cpu_of_process", kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("CPU load in percent")
        plot.set_title("CPU load of client process from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/CPU_of_process-" + client_names[i] + ".pdf")
        plt.close(fig)

        plot = resource_df.plot(y="ram_total", kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("RAM load in percent")
        plot.set_title("Total RAM load from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/ram_total-" + client_names[i] + ".pdf")
        plt.close(fig)

        resource_df["ram_of_process"] = resource_df["ram_of_process"] / 10**6
        plot = resource_df.plot(y="ram_of_process", kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("RAM usage in MB")
        plot.set_title("RAM usage of client process from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/ram_of_process-" + client_names[i] + ".pdf")
        plt.close(fig)

        col_names = ["bytes_sent", "bytes_rec", "packs_sent", "packs_rec"]
        dtype_dict.clear()
        dtype_dict = {"bytes_sent": int, "bytes_rec": int, "packs_sent": int, "packs_rec": int}
        network_df = pandas.read_csv(dirs["raw_dir"] + "/" + client_names[i] + "-network.csv", names=col_names,
                                      dtype=dtype_dict, header=None)

        plot = network_df.plot(y=col_names[0:2], kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("bytes")
        plot.set_title("Bytes from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/bytes-" + client_names[i] + ".pdf")
        plt.close(fig)

        plot = network_df.plot(y=col_names[2:], kind="line", linewidth=0.5)
        plot.set_xlabel("run time in seconds")
        plot.set_ylabel("packets")
        plot.set_title("Packets from " + client_names[i])
        fig = plot.get_figure()
        fig.savefig(result_dir + "/packets-" + client_names[i] + ".pdf")
        plt.close(fig)

        cpu_total_mean = []
        cpu_total_standard_deviation = []
        for j in range(columns - 3):
            cpu_total_mean.append(resource_df["cpu_total_core{0}".format(j)].mean())
            cpu_total_standard_deviation.append(str(math.sqrt(resource_df.loc[:, "cpu_total_core{0}".format(j)].var())))
        cpu_process_mean = str(resource_df["cpu_of_process"].mean())
        cpu_process_standard_deviation = str(math.sqrt(resource_df.loc[:, "cpu_of_process"].var()))
        ram_total_mean = str(resource_df["ram_total"].mean())
        ram_total_standard_deviation = str(math.sqrt(resource_df.loc[:, "ram_total"].var()))
        ram_process_mean = str(resource_df["ram_of_process"].mean())
        ram_process_standard_deviation = str(math.sqrt(resource_df.loc[:, "ram_of_process"].var()))

        output_text = ""
        for j in range(len(cpu_total_mean)):
            output_text += "Mean of total CPU load core{0}: ".format(j) + str(cpu_total_mean[j]) + "\n" + \
                "Standard deviation of total CPU load core{0}: ".format(j) + str(cpu_total_standard_deviation[j]) + "\n"
        with open(result_dir + "/Summary.txt", mode="a") as file:
            s = "Results from: " + client_names[i] + "\n\n" + output_text + \
                "Mean of CPU load of process: " + str(cpu_process_mean) + "\n" + \
                "Standard deviation of CPU load of process: " + str(cpu_process_standard_deviation) + "\n" + \
                "Mean of total RAM load: " + str(ram_total_mean) + "\n" + \
                "Standard deviation of total RAM load: " + str(ram_total_standard_deviation) + "\n" + \
                "Mean of RAM usage of process (MB): " + str(ram_process_mean) + "\n" + \
                "Standard deviation of RAM usage of process (MB): " + str(ram_process_standard_deviation) + "\n\n" + \
                "Total received messages: " + str(client_rec_messages) + "\n" + \
                "Total sent messages: " + str(client_sent_messages) + "\n" + \
                "Average received messages: " + str(avg_rec_messages) + "\n" + \
                "Average sent messages: " + str(avg_sent_messages) + "\n" + \
                "Mean Latency (ms): " + client_latency_mean[0] + "\n" + \
                "Median Latency (ms): " + client_latency_median[0] + "\n" + \
                "Minimum Latency (ms): " + client_latency_min[0] + "\n" + \
                "Maximum Latency (ms): " + client_latency_max[0] + "\n" + \
                "Latency Standard Deviation (ms): " + client_latency_sd[0] + "\n" + \
                "Mean Latency (ms) of sent messages: " + client_latency_mean[1] + "\n" + \
                "Median Latency (ms) of sent messages: " + client_latency_median[1] + "\n" + \
                "Minimum Latency (ms) of sent messages: " + client_latency_min[1] + "\n" + \
                "Maximum Latency (ms) of sent messages: " + client_latency_max[1] + "\n" + \
                "Latency Standard Deviation (ms) of sent messages: " + client_latency_sd[1] + "\n" + \
                "Mean Latency (ms) of received messages: " + client_latency_mean[2] + "\n" + \
                "Median Latency (ms) of received messages: " + client_latency_median[2] + "\n" + \
                "Minimum Latency (ms) of received messages: " + client_latency_min[2] + "\n" + \
                "Maximum Latency (ms) of received messages: " + client_latency_max[2] + "\n" + \
                "Latency Standard Deviation (ms) of received messages: " + client_latency_sd[2] + "\n\n"
            file.write(s)
            warnings = open(dirs["raw_dir"] + '/' + client_names[i] + '-warning.log', 'r').read()
            if warnings is not "":
                s = "Warnings:\n" + warnings + "\n\n"
                file.write(s)

        csv_row = [client_names[i]]
        for j in range(max_number_of_cores):
            csv_row.append(None)
            csv_row.append(None)
        for j in range(columns - 3):
            csv_row[2 * j + 1] = cpu_total_mean[j]
            csv_row[2 * j + 2] = cpu_total_standard_deviation[j]
        csv_row.extend([cpu_process_mean, cpu_process_standard_deviation, ram_total_mean, ram_total_standard_deviation,
                        ram_process_mean, ram_process_standard_deviation, client_rec_messages, client_sent_messages,
                        avg_rec_messages, avg_sent_messages, client_latency_mean[0], client_latency_median[0],
                        client_latency_min[0], client_latency_max[0], client_latency_sd[0], client_latency_mean[1],
                        client_latency_median[1], client_latency_min[1], client_latency_max[1], client_latency_sd[1],
                        client_latency_mean[2], client_latency_median[2], client_latency_min[2], client_latency_max[2],
                        client_latency_sd[2], None, None])

        with open(result_dir + "/Summary.csv", mode="a") as file:
            csv_writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csv_writer.writerow(csv_row)

    with open(result_dir + "/Summary.txt", mode="a") as file:
        s = "\n##### Messaging Results ######\n"
        file.write(s)

    send_dataframe = send_dataframe.sort_values(by="timestamp")
    rec_dataframe = rec_dataframe.sort_values(by="timestamp")
    fig, ax = plt.subplots()
    rec_messages = len(rec_dataframe.index)
    send_messages = len(send_dataframe.index)
    ax.plot(send_dataframe["runtime"], range(1, send_messages + 1), linewidth=0.5, label="sent messages")
    ax.plot(rec_dataframe["runtime"], range(1, rec_messages + 1), linewidth=0.5, label="received messages")
    ax.set_xlabel("seconds")
    ax.set_ylabel("number of messages")
    ax.set_title("Received and sent messages from all clients")
    plt.legend()
    fig.savefig(result_dir + "/messages-allClients.pdf")
    plt.close(fig)

    send_dataframe.to_csv(result_dir + "/sentMessages-allClients.csv")
    rec_dataframe.to_csv(result_dir + "/recMessages-allClients.csv")

    # The dataframes with the sent and received messages are merged together using the uuids from the messages.
    # This will create one entry for every received message with the corresponding sent message.
    merged_dataframe = pandas.merge(send_dataframe, rec_dataframe, how="left", on="uuid")

    # The latency is computed in microseconds for every received message and then sorted by the timestamps from the
    # sent message.
    merged_dataframe['latency'] = (merged_dataframe['timestamp_y'] - merged_dataframe['timestamp_x']) / 10**6
    merged_dataframe = merged_dataframe.sort_values(by=["timestamp_x"])

    # The first and last 60 seconds are cut off using the timestamp from the sent messages.
    # merged_dataframe = merged_dataframe.loc[merged_dataframe["timestamp_x"].between(int((run_info[1] + 60) * 10 ** 9),
    # int((run_info[1] + run_info[0] - 60) * 10 ** 9))]

    # The average throughput of received messages is computed and the graph for the latency values. The graph and the
    # values used in the graph are stored in the result directory.
    avg_rec_throughput = rec_messages / ((run_info[0]) / 60)

    latency_df = merged_dataframe[["runtime_x", "latency"]]
    latency_df.dropna()

    plot = latency_df.plot.scatter(y='latency', x='runtime_x', s=2)
    plot.set_xlabel("time (sent) in s")
    plot.set_ylabel("latency in milliseconds")
    plot.set_title("Latency from all clients")
    fig = plot.get_figure()
    fig.savefig(result_dir + "/latency-allClients.pdf")
    plt.close(fig)

    latency_df.to_csv(result_dir + "/latency-allClients.csv", columns=["runtime_x", "latency"], index=False)

    # The latency metrics are computed and also written in the Summary file.
    latency_mean = str(merged_dataframe["latency"].mean())
    latency_median = str(merged_dataframe["latency"].median())
    latency_min = str(merged_dataframe["latency"].min())
    latency_max = str(merged_dataframe["latency"].max())
    latency_standard_deviation = str(math.sqrt(merged_dataframe.loc[:, "latency"].var()))

    with open(result_dir + "/Summary.txt", mode="a") as file:
        s = "\nMean Latency (ms): " + latency_mean + \
            "\nMedian Latency (ms): " + latency_median + \
            "\nMinimum Latency (ms): " + latency_min + \
            "\nMaximum Latency (ms): " + latency_max + \
            "\nLatency Standard Deviation (ms): " + latency_standard_deviation
        file.write(s)

    # The dataframe is grouped by the uuids (sent messages) and the received messages for every uuid are counted.
    # For every sent message the dataframe now contains the uuid, the message type, the timestamp from the sent message,
    # and the count of received messages.
    merged_dataframe = merged_dataframe.groupby(['uuid', 'message_x']).agg({"type_y": "count", "timestamp_x": "mean", "runtime_x": "mean"})[
        ["timestamp_x", "runtime_x", "type_y"]]
    # The average throughput for the sent messages is computed.
    avg_sen_throughput = send_messages / ((run_info[0]) / 60)

    merged_dataframe["losses"] = 0

    for topic in topic_receivers.keys():
        merged_dataframe.loc[(merged_dataframe.index.get_level_values('message_x') == topic), "losses"] = topic_receivers[topic] \
        - merged_dataframe[(merged_dataframe.index.get_level_values('message_x') == topic)].type_y

    # The cumulative sum of losses is computed and the graph for the message losses is computed and saved in the result
    # directory with the values that were used to compute these graphs.
    merged_dataframe["sum of losses"] = merged_dataframe.losses.cumsum()
    plot = merged_dataframe.plot(y="sum of losses", x="runtime_x", kind="line", linewidth=0.5)
    plot.set_xlabel("time (sent) in s")
    plot.set_ylabel("number of losses")
    plot.set_title("Lost messages from all clients")
    fig = plot.get_figure()
    fig.savefig(result_dir + "/losses-allClients.pdf")
    plt.close(fig)

    merged_dataframe.to_csv(result_dir + "/losses-allClients.csv", columns=["timestamp_x", "runtime_x", "losses", "sum of losses"])

    # This computes and saves all metrics concerning the message counts in the Summary file.
    total_losses = merged_dataframe["losses"].sum()
    percent_losses = total_losses / (rec_messages + total_losses)

    with open(result_dir + "/Summary.txt", mode="a") as file:
        s = "\nTotal Number of Sent Messages: " + str(send_messages) + \
            "\nTotal Number of Received Messages: " + str(rec_messages) + \
            "\nAverage Sent Messages per Minute: " + str(avg_sen_throughput) + \
            "\nAverage Received Messages per Minute: " + str(avg_rec_throughput) + \
            "\nLost Messages: " + str(total_losses) + \
            "\nPercentage of Lost Messages: " + str(percent_losses)
        file.write(s)

    row_length = len(csv_row)
    csv_row_last = [None] * row_length
    csv_row_last[0] = "all_clients"
    csv_row_last[2 * max_number_of_cores + 7] = rec_messages
    csv_row_last[2 * max_number_of_cores + 8] = send_messages
    csv_row_last[2 * max_number_of_cores + 9] = avg_rec_throughput
    csv_row_last[2 * max_number_of_cores + 10] = avg_sen_throughput
    csv_row_last[-1] = percent_losses
    csv_row_last[-2] = total_losses
    csv_row_last[-13] = latency_standard_deviation
    csv_row_last[-14] = latency_max
    csv_row_last[-15] = latency_min
    csv_row_last[-16] = latency_median
    csv_row_last[-17] = latency_mean

    with open(result_dir + "/Summary.csv", mode="a") as file:
        csv_writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(csv_row_last)

    return "Evaluation complete"


def get_id_of_latest_run():
    run_count = 0
    temp_path = Path().absolute().parent.joinpath(Path("benchmark_run_{0}".format(run_count)))
    while os.path.exists(temp_path):
        run_count += 1
        temp_path = Path().absolute().parent.joinpath(Path("benchmark_run_{0}".format(run_count)))
    return run_count - 1


def get_path_to_specific_run(run_id):
    path_to_dir = Path().absolute().parent.joinpath(Path("benchmark_run_{0}".format(run_id)))
    if os.path.exists(path_to_dir):
        return str(path_to_dir)
    else:
        return None


@app.get("/results/all_results")
def get_all_results():
    run_id = get_id_of_latest_run()
    return get_all_results(run_id)


@app.get("/results/all_results/{run_id}")
def get_all_results(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if os.path.exists(path_to_dir):
        shutil.make_archive(str(Path().absolute().parent) + "/results", "zip", path_to_dir)
        filename = "all_results_run_" + str(run_id) + ".zip"
        return FileResponse(str(Path().absolute().parent) + "/results.zip", filename=filename)
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/configuration")
def get_configuration():
    run_id = get_id_of_latest_run()
    return get_configuration(run_id)


@app.get("/results/configuration/{run_id}")
def get_configuration(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/configuration.json")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/raw_data/messages")
def get_raw_files():
    run_id = get_id_of_latest_run()
    return get_raw_timestamps(run_id)


@app.get("/raw_data/messages/{run_id}")
def get_raw_timestamps(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir + "/raw_data")]
        if not os.path.exists(path_to_dir + "/raw_data/timestamps.zip"):
            with zipfile.ZipFile(path_to_dir + '/raw_data/timestamps.zip', 'w') as myzip:
                for file in files:
                    if "resources" not in file:
                        myzip.write(path_to_dir + "/raw_data/" + file, file)
        return FileResponse(path_to_dir + "/raw_data/timestamps.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/raw_data/resources")
def get_raw_resources():
    run_id = get_id_of_latest_run()
    return get_raw_resources(run_id)


@app.get("/raw_data/resources/{run_id}")
def get_raw_resources(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir + "/raw_data")]
        if not os.path.exists(path_to_dir + "/raw_data/resources.zip"):
            with zipfile.ZipFile(path_to_dir + '/raw_data/resources.zip', 'w') as myzip:
                for file in files:
                    if "resources" in file:
                        myzip.write(path_to_dir + "/raw_data/" + file, file)
        return FileResponse(path_to_dir + "/raw_data/resources.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/raw_date/network")
def get_raw_network():
    run_id = get_id_of_latest_run()
    return get_raw_network(run_id)


@app.get("/raw_data/network/{run_id}")
def get_raw_network(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir + "/raw_data")]
        if not os.path.exists(path_to_dir + "/raw_data/network.zip"):
            with zipfile.ZipFile(path_to_dir + '/raw_data/network.zip', 'w') as myzip:
                for file in files:
                    if "network" in file:
                        myzip.write(path_to_dir + "/raw_data/" + file, file)
        return FileResponse(path_to_dir + "/raw_data/network.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/cpu_graphs")
def get_cpu_graph():
    run_id = get_id_of_latest_run()
    return get_cpu_graph(run_id)


@app.get("/results/cpu_graphs/{run_id}")
def get_cpu_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/cpu_graph.zip"):
            with zipfile.ZipFile(path_to_dir + '/cpu_graph.zip', 'w') as myzip:
                for file in files:
                    if "CPU" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/resources.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/ram_graphs")
def get_ram_graph():
    run_id = get_id_of_latest_run()
    return get_cpu_graph(run_id)


@app.get("/results/ram_graphs/{run_id}")
def get_ram_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/ram_graph.zip"):
            with zipfile.ZipFile(path_to_dir + '/ram_graph.zip', 'w') as myzip:
                for file in files:
                    if "ram" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/resources.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/latency_total_graph")
def get_total_latency_graph():
    run_id = get_id_of_latest_run()
    return get_total_latency_graph(run_id)


@app.get("/results/latency_total_graph/{run_id}")
def get_total_latency_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    return FileResponse(path_to_dir + "/latency-allClients.pdf")


@app.get("/results/latency_total_csv")
def get_total_latency_csv():
    run_id = get_id_of_latest_run()
    return get_total_latency_csv(run_id)


@app.get("/results/latency_total_csv/{run_id}")
def get_total_latency_csv(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    return FileResponse(path_to_dir + "/latency-allClients.csv")


@app.get("/results/latency_{client_name}_graph")
def get_single_client_latency_graph(client_name):
    run_id = get_id_of_latest_run()
    return get_single_client_latency_graph(run_id, client_name)


@app.get("/results/latency_{client_name}_graph/{run_id}")
def get_single_client_latency_graph(run_id, client_name):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/latency-" + client_name + "_graph.zip"):
            with zipfile.ZipFile(path_to_dir + "/latency-" + client_name + "_graph.zip", 'w') as myzip:
                for file in files:
                    if "latency" in file and client_name in file and ".pdf" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/latency-" + client_name + "_graph.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/latency_all_clients_graph")
def get_every_client_latency_graph():
    run_id = get_id_of_latest_run()
    return get_every_client_latency_graph(run_id)


@app.get("/results/latency_all_clients_graph/{run_id}")
def get_every_client_latency_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/latency-allClients-graph.zip"):
            with zipfile.ZipFile(path_to_dir + "/latency-allClients-graph.zip", 'w') as myzip:
                for file in files:
                    if "latency-Client" in file and ".pdf" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/latency-allClients_graph.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("results/latency_{client_name}_csv")
def get_single_client_latency_csv(client_name):
    run_id = get_id_of_latest_run()
    return get_single_client_latency_csv(run_id, client_name)


@app.get("results/latency_{client_name}_csv/{run_id}")
def get_single_client_latency_csv(run_id, client_name):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/latency-" + client_name + "_csv.zip"):
            with zipfile.ZipFile(path_to_dir + "/latency-" + client_name + "_csv.zip", 'w') as myzip:
                for file in files:
                    filename = "latency-{0}".format(client_name)
                    if filename in file and ".csv" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/latency-" + client_name + "_csv.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/latency_all_clients_csv")
def get_every_client_latency_csv():
    run_id = get_id_of_latest_run()
    return get_every_client_latency_csv(run_id)


@app.get("/results/latency_all_clients_csv/{run_id}")
def get_every_client_latency_csv(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/latency-allClients-csv.zip"):
            with zipfile.ZipFile(path_to_dir + "/latency-allClients-csv.zip", 'w') as myzip:
                for file in files:
                    if "latency-Client" in file and ".csv" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/latency-allClients_csv.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/messages_total_graph")
def get_total_messages_graph():
    run_id = get_id_of_latest_run()
    return get_total_messages_graph(run_id)


@app.get("/results/messages_total_graph/{run_id}")
def get_total_messages_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/messages-allClients.pdf")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/messages_{client_name}_graph")
def get_single_client_messages_graph(client_name):
    run_id = get_id_of_latest_run()
    return get_single_client_messages_graph(run_id, client_name)


@app.get("/results/messages_{client_name}_graph/{run_id}")
def get_single_client_messages_graph(run_id, client_name):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        if os.path.exists(path_to_dir + "/results/messages-" + client_name + ".pdf"):
            return FileResponse(path_to_dir + "/results" + "/messages-allClients.pdf")
        else:
            return "Client " + client_name + " does not exist!"
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/messages_all_clients_graph")
def get_every_client_messages_graph(client_name):
    run_id = get_id_of_latest_run()
    return get_every_client_messages_graph(run_id, client_name)


@app.get("/results/messages_all_clients_graph/{run_id}")
def get_every_client_messages_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id) + "/results"
    if path_to_dir is not None:
        files = [f for f in listdir(path_to_dir)]
        if not os.path.exists(path_to_dir + "/messages-allClients-graph.zip"):
            with zipfile.ZipFile(path_to_dir + "/messages-allClients-graph.zip", 'w') as myzip:
                for file in files:
                    if "messages-Client" in file:
                        myzip.write(path_to_dir + "/" + file, file)
        return FileResponse(path_to_dir + "/messages-allClients_graph.zip")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/losses_total_graph")
def get_total_losses_graph():
    run_id = get_id_of_latest_run()
    return get_total_losses_graph(run_id)


@app.get("/results/losses_total_graph/{run_id}")
def get_total_losses_graph(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/losses-allClients.pdf")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/losses_total_csv")
def get_total_losses_csv():
    run_id = get_id_of_latest_run()
    return get_total_losses_csv(run_id)


@app.get("/results/losses_total_csv/{run_id}")
def get_total_losses_csv(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/losses-allClients.csv")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/messages_total_received_csv")
def get_total_rec_messages():
    run_id = get_id_of_latest_run()
    return get_total_rec_messages(run_id)


@app.get("/results/messages_total_received_csv/{run_id}")
def get_total_rec_messages(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/recMessages-allClients.csv")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/messages_total_sent_csv")
def get_total_sent_messages():
    run_id = get_id_of_latest_run()
    return get_total_sent_messages(run_id)


@app.get("/results/messages_total_sent_csv/{run_id}")
def get_total_sent_messages(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/sentMessages-allClients.csv")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/summary.txt")
def get_summary_txt():
    run_id = get_id_of_latest_run()
    return get_summary_txt(run_id)


@app.get("/results/summary.txt/{run_id}")
def get_summary_txt(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/Summary.txt")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


@app.get("/results/summary.csv")
def get_summary_csv():
    run_id = get_id_of_latest_run()
    return get_summary_csv(run_id)


@app.get("/results/summary.csv/{run_id}")
def get_summary_csv(run_id):
    path_to_dir = get_path_to_specific_run(run_id)
    if path_to_dir is not None:
        return FileResponse(path_to_dir + "/results" + "/Summary.csv")
    else:
        return "Benchmark run {0} does not exist".format(run_id)


if __name__ == '__main__':
    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(serve(app, config))
