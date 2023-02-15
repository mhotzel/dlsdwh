from enum import IntEnum


class LogLevel(IntEnum):
    TRACE = 1
    INFO = 2
    WARN = 4
    ERROR = 8
    SEVERE = 16
