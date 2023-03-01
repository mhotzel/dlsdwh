from queue import Queue
from threading import Thread
from tkinter.filedialog import askopenfilenames
from typing import List, Protocol, Tuple, Type

from controller.controller import Controller
from model.db_manager import DbManager
from model.errors import DatenImportError
from model.kassenjournal import KassenjournalImporter
from model.log_level import LogLevel


class Importer(Protocol):
    '''
    Interface für Importer
    '''

    @property
    def import_file(self) -> str:
        '''Liefert die zu importierende Datei zurück'''
        ...

    @import_file.setter
    def import_file(self, import_file: str) -> None:
        '''Setzt die zu importierende Datei'''
        ...

    def write_data(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''
        ...

    def load_file(self) -> None:
        '''
        Startet den Import des Kassenjournals in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle mittels ::update_table gestartet werden.
        '''
        ...

    def post_process(self) -> None:
        '''Nach der Beladung der Zwischentabelle wird mittels dieser Methode die Beladung der Zieltabelle gestartet.'''
        ...

    

class JobOwner(Protocol):
    '''
    Interface für Klassen und insbesondere Widgets, die Jobs
    starten und über das Ende des Jobs benachrichtigt werden wollen.
    Nach Beendigung des Jobs kann der JObOwner über den Aufruf der
    Methode 'done()' benachrichtigt werden, um z.B. die Buttons,
    die ggf. zum Start deaktiviert werden, wieder zu aktivieren.
    '''

    def done(self) -> None:
        '''Wird vom Job nach Beendigung aufgerufen'''
        ...

class ImportJobController():
    '''Job Controller für den Import von Dateien'''

    def __init__(self, application: Controller, job_owner: JobOwner, importer_clzz, db_manager: DbManager) -> None:
        super().__init__()

        self.application = application
        self.db_manager = db_manager
        self.filenames = None
        self.job_owner = job_owner
        self.importer_clzz = importer_clzz

    def importfile_ermitteln(self, filetypes: List[Tuple[str, str]], defaultextension: str) -> None:
        '''Ermittelt die zu importierende Datei'''

        self.filenames = askopenfilenames(
            title='Importdatei auswählen',
            defaultextension=defaultextension,
            filetypes=filetypes
        )

        if not self.filenames:
            self.job_owner.done()
            raise DatenImportError(f'Es wurde keine Eingabedatei gewählt')

        self.filenames = sorted(self.filenames)

    def starte_import(self) -> None:
        '''Startet den Import'''

        queue = Queue()
        worker = Thread(target=self._run_import, args=(
            self.importer_clzz, self.db_manager, self.filenames, queue, self.application))

        worker.start()
        self.application.after(1, lambda: self.monitor(worker, queue))

    def monitor(self, worker: Thread, queue: Queue) -> None:
        '''Ueberwacht den Thread und schreibt das Log'''
        if worker.is_alive():
            if not queue.empty():
                msg = queue.get()
                self.application.log_message(LogLevel.INFO, msg)

            self.application.after(50, lambda: self.monitor(worker, queue))
        else:
            if self.e:
                self.application.log_message(
                LogLevel.ERROR, f'Import mit Fehler beendet: {self.e}')
            else:
                self.application.log_message(
                    LogLevel.INFO, f'Import abgeschlossen')
            self.job_owner.done()

    def _run_import(self, importer_clzz: Type[Importer], db_man: DbManager, files: List[str], queue: Queue, app: Controller) -> None:
        '''Soll in einem Thread ausgeführt werden, verarbeitet die Dateien'''

        self.e = None
        try:
            for file in files:
                imp = importer_clzz(db_man, file)
                imp.load_file()
                queue.put(f"Datei '{file}' geladen. Schreiben gestartet...")
                imp.write_data()
                queue.put(f"Datei '{file}' geschrieben. Nachverarbeitung gestartet...")
                imp.post_process()
                queue.put(f"Datei '{file}' Nachverarbeitung abgeschlossen")
        except Exception as e:
            self.e = e
