from typing import Protocol
from model.log_level import LogLevel


class Controller(Protocol):
    '''Interface für den Controller'''

    def zeige_import_frame(self) -> None:
        '''Zeigt den Import-Frame an.'''
        ...

    def zeige_auswertungen_frame(self) -> None:
        '''Zeigt den Auswertungen-Frame an.'''
        ...

    def log_message(self, level: LogLevel, message: str) -> None:
        '''Loggt die uebergebene Nachricht.'''
