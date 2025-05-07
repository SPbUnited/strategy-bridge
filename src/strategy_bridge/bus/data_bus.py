import attr
import typing

from collections import deque

from strategy_bridge.bus.record import Record

import zmq
import threading

DB_TRANSPORT = "ipc://"
DB_READ_SUFFIX = "_read"
DB_WRITE_SUFFIX = "_write"
DB_PROXY_SUFFIX = ".proxy"

DB_READ_PREFFIX_XPUB = DB_TRANSPORT + "/tmp/xpub_"
DB_WRITE_PREFFIX_XSUB = DB_TRANSPORT + "/tmp/xsub_"


@attr.s(auto_attribs=True)
class DataBus:
    transport: str = "inproc://"
    read_suffix: str = "_read"
    write_suffix: str = "_write"
    proxy_suffix: str = ".proxy"

    context: zmq.Context = attr.ib(factory=zmq.Context)
    sockets: typing.Dict[str, zmq.Socket] = attr.ib(init=False)

    def __attrs_post_init__(self):
        # self.data: typing.Dict[str, deque[Record]] = {}
        self.sockets = {}

    def register_topic(self, topic_name: str, max_size: int) -> None:
        print("Registering topic", topic_name)

        if self.transport + topic_name + self.write_suffix in self.sockets:
            print("Topic" + topic_name + self.write_suffix + " already registered")
            return

        read_name_sub = self.transport + topic_name + self.read_suffix
        write_name_pub = self.transport + topic_name + self.write_suffix
        read_proxy_name_xsub = (
            self.transport + topic_name + self.read_suffix + self.proxy_suffix
        )
        write_proxy_name_xpub = (
            self.transport + topic_name + self.write_suffix + self.proxy_suffix
        )

        print("Creating sockets")
        self.sockets[read_name_sub] = self.context.socket(zmq.SUB)
        self.sockets[write_name_pub] = self.context.socket(zmq.PUB)
        self.sockets[read_proxy_name_xsub] = self.context.socket(zmq.XSUB)
        self.sockets[write_proxy_name_xpub] = self.context.socket(zmq.XPUB)

        print("Binding sockets")
        self.sockets[read_proxy_name_xsub].bind(write_name_pub)
        self.sockets[write_proxy_name_xpub].bind(read_name_sub)
        self.sockets[read_name_sub].connect(read_name_sub)
        self.sockets[write_name_pub].connect(write_name_pub)

        print("Subscribing to topic")
        self.sockets[read_name_sub].setsockopt_string(zmq.SUBSCRIBE, "")

        threading.Thread(
            target=lambda xsub, xpub: zmq.proxy(xsub, xpub),
            args=(
                self.sockets[read_proxy_name_xsub],
                self.sockets[write_proxy_name_xpub],
            ),
        ).start()
        # self.data[topic_name] = deque(maxlen=max_size)

    def write(self, topic_name: str, record: Record) -> None:
        # self.data[topic_name].append(record)
        self.sockets[self.transport + topic_name + self.write_suffix].send_pyobj(record)

    def read_all(self, topic_name) -> typing.List[Record]:
        data = []
        for _ in range(10):
            try:
                data.append(
                    self.sockets[
                        self.transport + topic_name + self.read_suffix
                    ].recv_pyobj(flags=zmq.NOBLOCK)
                )
            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    break
                else:
                    raise e

        if not data:
            print(f"No data in topic {topic_name}")

        # data = None
        # data = self.data.get(topic_name)
        # if not data:
        #     print(f"No data in topic {topic_name}")
        #     return []
        # return list(data)
        return data

    def read_top(self, topic_name: str, count: int) -> typing.List[Record]:
        return self.read_all(topic_name)[-count:]

    def read_from_timestamp(
        self, topic_name: str, timestamp: float
    ) -> typing.List[Record]:
        records = self.read_all(topic_name)
        valid_records = [r for r in records if r.timestamp > timestamp]
        return valid_records
