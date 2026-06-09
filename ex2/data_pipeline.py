from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[Any] = []
        self.index = 0
        self.name = "Data Processor"
        self.total_proc = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        output_tuple = (self.index, self.storage[0])
        self.index += 1
        self.storage.remove(self.storage[0])
        return output_tuple


class NumericProcessor(DataProcessor):
    def __init__(self):
        super().__init__()
        self.name = "Numeric Processor"

    def validate(self, data: int | float | list[int | float]) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        valid = self.validate(data)
        if valid:
            if isinstance(data, (int, float)):
                new_data = str(data)
                self.storage.append(new_data)
                self.total_proc += 1
            else:
                for item in data:
                    new_item = str(item)
                    self.storage.append(new_item)
                    self.total_proc += 1
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):

    def __init__(self):
        super().__init__()
        self.name = "Text Processor"

    def validate(self, data: str | list[str]) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for st in data:
                if not isinstance(st, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        valid = self.validate(data)

        if valid:
            if isinstance(data, str):
                self.storage.append(data)
                self.total_proc += 1
            else:
                for item in data:
                    self.storage.append(item)
                    self.total_proc += 1
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):

    def __init__(self):
        super().__init__()
        self.name = "Log Processor"

    def validate(self, data: dict[str, str] | list[dict[str, str]]) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, str):
                    return False
            return True
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    for key, value in item.items():
                        if (not isinstance(key, str) or
                                not isinstance(value, str)):
                            return False
                else:
                    return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        valid = self.validate(data)

        if valid:
            if isinstance(data, dict):
                for key, value in data.items():
                    res = key + ": " + value
                    self.storage.append(res)
                    self.total_proc += 1
            else:
                for item in data:
                    self.storage.append(item)
                    self.total_proc += 1
        else:
            raise ValueError("Improper log data")


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = 0
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = 1
            if handled == 0:
                print(f"DataStream error - Can't process element in stream: \
{item}")

    def print_processors_stats(self) -> None:
        if len(self.processors) == 0:
            print("No processor found, no data")
            return
        for proc in self.processors:
            print(f"{proc.name}: total {proc.total_proc} item processed, \
remaining {len(proc.storage)} on processor")


class ExportPlugin(Protocol):

    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVClass:

    def process_output(self, data: list[tuple[int, str]]) -> None:
        res_list = []
        sep = ","
        for item in data:
            res_list.append(item[1])
        res_str = sep.join(res_list)
        print("CSV Output:")
        print(res_str)


def data_pipeline() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    ds = DataStream()
    print("== DataStream statistics ==")
    ds.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    np = NumericProcessor()
    ts = TextProcessor()
    ls = LogProcessor()
    ds.register_processor(np)
    ds.register_processor(ts)
    ds.register_processor(ls)
    first_batch = ['Hello world', [3.14, -1, 2.71],
                   [{'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO', 'log_message':
                     'User wil is connected'}], 42, ['Hi', 'five']]

    print(f"Send first batch of data on stream: {first_batch}")
    ds.process_stream(first_batch)

    print("== DataStream statistics ==")
    ds.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CDV plugin:")
    print("CSV Output:")
    data = []
    for proc in ds.processors:
        data.append(proc.output())
        print(data)
    csv = CSVClass()
    csv.process_output(data)
        

if __name__ == "__main__":
    data_pipeline()
