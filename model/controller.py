

from typing import Callable


class Controller:
    '''Der Anwendungscontroller'''

    EVT_ZEIGE_IMPORT_FRAME = 1
    EVT_ZEIGE_AUSWERTUNGEN_FRAME = 2

    def __init__(self) -> None:
        self._listeners = {}

    def get_listener(self, evt_type: int) -> Callable:
        '''Ermittelt die Event Listener des uebergebenen Typs. Wenn keiner existiert, wird ein leeres Set zurueck gegeben '''

        if not evt_type in self._listeners:
            return set()

        return self._listeners[evt_type]

    def register_listener(self, evt_type: int, func: Callable) -> None:
        '''Registriert einen Listener zum angegebenen Eventtyp'''
        if not evt_type in self._listeners:
            self._listeners[evt_type] = set()

        self._listeners[evt_type].add(func)

    def zeige_import_frame(self) -> None:
        '''Zeigt den Import-Frame an.'''

        for func in self.get_listener(self.EVT_ZEIGE_IMPORT_FRAME):
            func()

    def zeige_auswertungen_frame(self) -> None:
        '''Zeigt den Auswertungen-Frame an.'''

        for func in self.get_listener(self.EVT_ZEIGE_AUSWERTUNGEN_FRAME):
            func()

    def import_kassenjournal(self) -> None:
        '''Startet den Import des Kassenjournals'''
        print(f"TODO: Implementierung import Kassenjournal; {__file__}")