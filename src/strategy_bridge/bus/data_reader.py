import typing

import attr

from strategy_bridge.bus import Record, DataBus
from .data_bus import DB_READ_PREFFIX_XPUB, DB_WRITE_PREFFIX_XSUB

import zmq


@attr.s(auto_attribs=True)
class DataReader:

    data_bus: DataBus
    read_topic_name: str
    last_read_message_timestamp: float = 0

    def __attrs_post_init__(self):
        print("Creating reader for topic", self.read_topic_name)
        self.context = zmq.Context()

        read_proxy_name_xsub = DB_WRITE_PREFFIX_XSUB + self.read_topic_name
        write_proxy_name_xpub = DB_READ_PREFFIX_XPUB + self.read_topic_name

        self.s_reader = self.context.socket(zmq.SUB)
        self.s_reader.connect(write_proxy_name_xpub)
        self.s_reader.setsockopt_string(zmq.SUBSCRIBE, "")

    def read_new(self) -> typing.List[Record]:
        records = []
        for _ in range(10):
            try:
                records.append(self.s_reader.recv_pyobj(flags=zmq.NOBLOCK))
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                else:
                    raise e

        # records = self.data_bus.read_from_timestamp(
        #     self.read_topic_name, self.last_read_message_timestamp
        # )
        # if records:
        #     self.last_read_message_timestamp = records[-1].timestamp
        return records

    def read_last(self) -> typing.Optional[Record]:
        record = None
        for _ in range(10):
            try:
                record = self.s_reader.recv_pyobj(flags=zmq.NOBLOCK)
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                else:
                    raise e
        return record
        # try:
        #     return self.read_new()[-1]
        # except IndexError:
        #     return None

        # record = self.data_bus.read_top(self.read_topic_name, 1)
        # if record:
        #     return record[0]
        # return None

    def read_all(self) -> typing.List[Record]:
        # return self.data_bus.read_all(self.read_topic_name)
        return self.read_new()
