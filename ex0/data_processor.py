from abc import ABC, abstractmethod
from typing import Any, cast


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[Any] = []
        self.index = 0

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
            else:
                for item in data:
                    new_item = str(item)
                    self.storage.append(new_item)
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):

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
            else:
                for item in data:
                    self.storage.append(item)
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):

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
                self.storage.append(data)
            else:
                for item in data:
                    self.storage.append(item)
        else:
            raise ValueError("Improper log data")


def data_processor() -> None:
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('hello')}")
    print(" Test invalid ingestion of string 'foo' without prior \
validation:")
    try:
        numeric.ingest("foo")
    except ValueError as v:
        print(f" Got exception: {v}")
    num_data = [1, 2, 3, 4, 5]
    numeric.ingest(num_data)
    print(f" Processing data: {num_data}")

    print(" Extracting 3 values...")
    for i in range(3):
        num_output = numeric.output()
        print(f" Numeric value {num_output[0]}: {num_output[1]}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f" Trying to validate input '42': {text.validate(42)}")
    text_data = ['Hello', 'Nexus', 'World']
    text.ingest(text_data)
    print(f" Processing data: {text_data}")
    print(" Extracting 1 value...")
    text_output = text.output()
    print(f" Text value {text_output[0]}: {text_output[1]}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    log_data = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    log.ingest(log_data)
    print(f" Processing data: {log_data}")
    print(" Extraction 2 values...")
    for i in range(2):
        log_output = log.output()
        log_dict = cast(dict[str, str], log_output[1])
        print(f" Log entry {log_output[0]}: \
{log_dict['log_level']}: {log_dict['log_message']}")


if __name__ == "__main__":
    data_processor()
