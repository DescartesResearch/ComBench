import logging
import csv
import io
import os
import psutil
import time


def setup_logger(name, log_file, formatter, level=logging.INFO):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


class CsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_MINIMAL)

    def format(self, record):
        row = []
        if isinstance(record.msg[0], list):
            for value in record.msg[0]:
                row.append(value)
        else:
            row.append(record.msg[0])
        row.extend([record.msg[1], record.msg[2], record.msg[3]])
        self.writer.writerow(row)
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


class Measurements:

    def __init__(self, filename, devices):
        file = open('{0}.csv'.format(filename), 'w')
        file.truncate(0)
        file.close()
        file = open('{0}resources.csv'.format(filename), 'w')
        file.truncate(0)
        file.close()
        file = open('{0}network.csv'.format(filename), 'w')
        file.truncate(0)
        file.close()
        self.measure_logger = setup_logger("measure_logger", "{0}.csv".format(filename), CsvFormatter())
        self.resource_logger = setup_logger("resource_logger", "{0}resources.csv".format(filename), CsvFormatter())
        self.network_logger = setup_logger("network_logger", "{0}network.csv".format(filename), CsvFormatter())
        self.devices = devices

    def measure_resources(self, runtime):
        pid = os.getpid()
        cpu_total = psutil.cpu_percent(percpu=True)
        p = psutil.Process(pid=pid)

        old_bytes_sent = psutil.net_io_counters(pernic=True)[self.devices[0]].bytes_sent
        old_bytes_rec = psutil.net_io_counters(pernic=True)[self.devices[1]].bytes_recv
        old_packs_sent = psutil.net_io_counters(pernic=True)[self.devices[0]].packets_sent
        old_packs_rec = psutil.net_io_counters(pernic=True)[self.devices[1]].packets_recv

        for x in range(runtime):
            cpu_total = psutil.cpu_percent(interval=1, percpu=True)
            cpu_of_process = p.cpu_percent()
            mem = p.memory_info()[0]
            percent_mem = psutil.virtual_memory().percent
            self.resource_logger.info([cpu_total, cpu_of_process, percent_mem, mem])

            new_bytes_sent = psutil.net_io_counters(pernic=True)[self.devices[0]].bytes_sent
            new_bytes_rec = psutil.net_io_counters(pernic=True)[self.devices[1]].bytes_recv
            new_packs_sent = psutil.net_io_counters(pernic=True)[self.devices[0]].packets_sent
            new_packs_rec = psutil.net_io_counters(pernic=True)[self.devices[1]].packets_recv
            self.network_logger.info([new_bytes_sent-old_bytes_sent, new_bytes_rec-old_bytes_rec,
                                      new_packs_sent-old_packs_sent, new_packs_rec-old_packs_rec])

            old_bytes_sent = new_bytes_sent
            old_bytes_rec = new_bytes_rec
            old_packs_sent = new_packs_sent
            old_packs_rec = new_packs_rec

    def register_received(self, identifier, timestamp):
        self.measure_logger.info(["r", identifier, timestamp, ""])

    def register_sent(self, identifier, timestamp, topic):
        self.measure_logger.info(["s", identifier, timestamp, topic])

    def register_info(self, type, identifier, timestamp, topic):
        self.measure_logger.info([type, identifier, timestamp, topic])
