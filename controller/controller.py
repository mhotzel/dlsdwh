from typing import Callable, Protocol
from model.db_manager import DbManager
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

    @property
    def db_manager(self) -> DbManager:
        '''liefert den Datenbankmanager'''
        ...

    def after(self, ms: int, func: Callable = None) -> None:
        '''Die after-Methode aus dem tkinter Event Loop'''
        ...