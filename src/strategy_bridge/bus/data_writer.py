import time

import typing

import attr
import zmq.devices

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

        prx = zmq.devices.ThreadProxy(zmq.XSUB, zmq.XPUB)
        prx.bind_in(read_proxy_name_xsub)
        prx.bind_out(write_proxy_name_xpub)
        prx.start()

        self.s_writer.connect(read_proxy_name_xsub)

    def write(self, content: typing.Any):
        record = Record(content, time.time())
        # self.data_bus.write(self.write_topic_name, record)
        self.s_writer.send_pyobj(record)
