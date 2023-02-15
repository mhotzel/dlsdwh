from datetime import datetime
from enum import IntEnum
from model.db_manager import DbManager

from typing import Callable
from model.log_level import LogLevel
from ttkbootstrap import Window

class RoutingEvent(IntEnum):
    EVT_ZEIGE_IMPORT_FRAME = 1
    EVT_ZEIGE_AUSWERTUNGEN_FRAME = 2
    EVT_LOG_MESSAGE = 3   


class Router:
    '''Der Anwendungscontroller'''

    def __init__(self, main_win: Window, dbfile: str) -> None:
        self._listeners = {}
        self.dbfile = dbfile
        self.main_win = main_win
        self.db_manager = DbManager(self.dbfile)

    def get_listener(self, evt_type: RoutingEvent) -> Callable:
        '''Ermittelt die Event Listener des uebergebenen Typs. Wenn keiner existiert, wird ein leeres Set zurueck gegeben '''

        if not evt_type in self._listeners:
            return set()

        return self._listeners[evt_type]

    def register_listener(self, evt_type: RoutingEvent, func: Callable) -> None:
        '''Registriert einen Listener zum angegebenen Eventtyp'''
        if not evt_type in self._listeners:
            self._listeners[evt_type] = set()

        self._listeners[evt_type].add(func)

    def zeige_import_frame(self) -> None:
        '''Zeigt den Import-Frame an.'''

        for func in self.get_listener(RoutingEvent.EVT_ZEIGE_IMPORT_FRAME):
            func()

    def zeige_auswertungen_frame(self) -> None:
        '''Zeigt den Auswertungen-Frame an.'''

        for func in self.get_listener(RoutingEvent.EVT_ZEIGE_AUSWERTUNGEN_FRAME):
            func()

    def log_message(self, level: LogLevel, message: str) -> None:
        '''Loggt die uebergebene Nachricht.'''

        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"{ts} - {level.name} - {message}"
        for l in self.get_listener(self.EVT_LOG_MESSAGE):
            l(msg)
