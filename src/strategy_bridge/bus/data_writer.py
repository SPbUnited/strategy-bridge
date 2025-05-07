import time

import typing

import attr

from strategy_bridge.bus import Record, DataBus
from .data_bus import DB_READ_PREFFIX_XPUB, DB_WRITE_PREFFIX_XSUB

import zmq
import threading


@attr.s(auto_attribs=True)
class DataWriter:

    data_bus: DataBus
    write_topic_name: str
    max_persisted_records_count: int

    def __attrs_post_init__(self):
        # self.data_bus.register_topic(
        #     self.write_topic_name, self.max_persisted_records_count
        # )
        print("Registering writer for topic", self.write_topic_name)
        self.context = zmq.Context()

        read_proxy_name_xsub = DB_WRITE_PREFFIX_XSUB + self.write_topic_name
        write_proxy_name_xpub = DB_READ_PREFFIX_XPUB + self.write_topic_name

        self.s_writer = self.context.socket(zmq.PUB)

        # s_xsub = self.context.socket(zmq.XSUB)
        # s_xsub.bind(read_proxy_name_xsub)
        # s_xpub = self.context.socket(zmq.XPUB)
        # s_xpub.bind(write_proxy_name_xpub)
        # threading.Thread(
        #     target=lambda xsub, xpub: zmq.proxy(xsub, xpub),
        #     args=(s_xsub, s_xpub),
        # ).start()

        try:
            s_xsub = self.context.socket(zmq.XSUB)
            s_xsub.bind(read_proxy_name_xsub)
            s_xpub = self.context.socket(zmq.XPUB)
            s_xpub.bind(write_proxy_name_xpub)
            threading.Thread(
                target=lambda xsub, xpub: zmq.proxy(xsub, xpub),
                args=(s_xsub, s_xpub),
            ).start()
        except zmq.ZMQError as e:
            if e.errno == zmq.EADDRINUSE:
                print("Proxy already running")
            else:
                raise e

        self.s_writer.connect(read_proxy_name_xsub)

    def write(self, content: typing.Any):
        record = Record(content, time.time())
        # self.data_bus.write(self.write_topic_name, record)
        self.s_writer.send_pyobj(record)
