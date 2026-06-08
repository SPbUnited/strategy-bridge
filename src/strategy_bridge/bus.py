import attr
import typing
from collections import deque
import time

@attr.s(auto_attribs=True)
class Record:
    content: typing.Any
    timestamp: float

@attr.s(auto_attribs=True)
class DataBus:
    def __attrs_post_init__(self):
        self.data: typing.Dict[str, deque[Record]] = {}

    def register_topic(self, topic_name: str, max_size: int) -> None:
        self.data[topic_name] = deque(maxlen=max_size)

    def write(self, topic_name: str, record: Record) -> None:
        self.data[topic_name].append(record)

    def read_all(self, topic_name) -> typing.List[Record]:
        data = self.data.get(topic_name)
        if not data:
            print(f"No data in topic {topic_name}")
            return []
        return list(data)

    def read_top(self, topic_name: str, count: int) -> typing.List[Record]:
        return self.read_all(topic_name)[-count:]

    def read_from_timestamp(self, topic_name: str, timestamp: float) -> typing.List[Record]:
        records = self.read_all(topic_name)
        valid_records = [r for r in records if r.timestamp > timestamp]
        return valid_records

@attr.s(auto_attribs=True)
class DataReader:

    data_bus: DataBus
    read_topic_name: str
    last_read_message_timestamp: float = 0

    def read_new(self) -> typing.List[Record]:
        records = self.data_bus.read_from_timestamp(
            self.read_topic_name, self.last_read_message_timestamp
        )
        if records:
            self.last_read_message_timestamp = records[-1].timestamp
        return records

    def read_last(self) -> typing.Optional[Record]:
        record = self.data_bus.read_top(self.read_topic_name, 1)
        if record:
            return record[0]
        return None

    def read_all(self) -> typing.List[Record]:
        return self.data_bus.read_all(self.read_topic_name)
@attr.s(auto_attribs=True)
class DataWriter:

    data_bus: DataBus
    write_topic_name: str
    max_persisted_records_count: int

    def __attrs_post_init__(self):
        self.data_bus.register_topic(self.write_topic_name, self.max_persisted_records_count)

    def write(self, content: typing.Any):
        record = Record(content, time.time())
        self.data_bus.write(self.write_topic_name, record)

