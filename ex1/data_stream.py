from abc import ABC, abstractmethod
from typing import Any, Sequence


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
        self.rank = 0
        self.name = "Data Processor"

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("Storage is empty, cannot remove data")
        data = self.storage.pop(0)
        return (data)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | Sequence[int | float]) -> None:
        valid = self.validate(data)
        if valid:
            if isinstance(data, (int, float)):
                new_data = str(data)
                self.storage.append((self.rank, new_data))
                self.rank += 1
            else:
                for item in data:
                    new_item = str(item)
                    self.storage.append((self.rank, new_item))
                    self.rank += 1
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

    def validate(self, data: Any) -> bool:
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
                self.storage.append((self.rank, str(data)))
                self.rank += 1
            else:
                for item in data:
                    self.storage.append((self.rank, str(item)))
                    self.rank += 1
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

    def validate(self, data: Any) -> bool:
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
                    self.storage.append((self.rank, f"{key}:{value}"))
                    self.rank += 1
            else:
                for item in data:
                    values = list(item.values())
                    t_values = f"{values[0]}: {values[1]}"
                    self.storage.append((self.rank, f"{t_values}"))
                    self.rank += 1
        else:
            raise TypeError("Improper log data")


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            if handled is False:
                print(f"DataStream error - Can't process element in stream: \
{item}")

    def print_processors_stats(self) -> None:
        if len(self.processors) == 0:
            print("No processor found, no data")
            return
        for proc in self.processors:
            print(f"{proc.name}: total {proc.rank} items processed, \
remaining {len(proc.storage)} on processor")


def data_stream() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    ds = DataStream()
    print("== DataStream statistics ==")
    ds.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    np = NumericProcessor()
    ds.register_processor(np)
    first_batch = ['Hello world', [3.14, -1, 2.71],
                   [{'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO', 'log_message':
                     'User wil is connected'}], 42, ['Hi', 'five']]

    print(f"Send first batch of data on stream: {first_batch}")
    ds.process_stream(first_batch)

    print("== DataStream statistics ==")
    ds.print_processors_stats()

    print("\nRegistering other data processors")
    ts = TextProcessor()
    ls = LogProcessor()
    ds.register_processor(ts)
    ds.register_processor(ls)
    print("Send the same batch again")
    ds.process_stream(first_batch)
    print("== DataStream statistics ==")
    ds.print_processors_stats()

    print("\nConsume some elements from the data processors: \
Numeric 3, Text 2, Log 1")
    for i in range(3):
        np.output()
    for i in range(2):
        ts.output()
    ls.output()
    print("== DataStream statistics ==")
    ds.print_processors_stats()


if __name__ == "__main__":
    data_stream()
