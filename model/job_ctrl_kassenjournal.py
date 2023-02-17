import time
from queue import Queue
from threading import Thread
from tkinter.filedialog import askopenfilenames
from typing import List

from controller.controller import Controller
from model.db_manager import DbManager
from model.errors import DatenImportError
from model.kassenjournal_importer import KassenjournalImporter
from model.log_level import LogLevel


class KjImportJobController():
    '''Job Controller für den Import des Kassenjournals'''

    def __init__(self, application: Controller) -> None:
        super().__init__()

        self.application = application
        self.filenames = None

    def importfile_ermitteln(self) -> None:
        '''Ermittelt die zu importierende Datei'''

        self.filenames = askopenfilenames(
            title='Kassenjournal-Datei auswählen',
            defaultextension='SCHAPFL-Kassenjournaldatei (*.csv)',
            filetypes=[
                ('Kassenjournal-Datei', '*.csv')
            ],
        )

        if not self.filenames:
            raise DatenImportError(f'Es wurde keine Eingabedatei gewählt')

        self.filenames = sorted(self.filenames)

    def starte_import(self) -> None:
        '''Startet den Import'''

        queue = Queue()

        worker = Thread(target=self._run_import, args=(
            self.application.db_manager, self.filenames, queue))
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
            self.application.log_message(LogLevel.INFO, 'Import Kassenjournal fertig')

    def _run_import(self, db_man: DbManager, files: List[str], queue: Queue) -> None:
        '''Soll in einem Thread ausgeführt werden, verarbeitet die Dateien'''

        conn = db_man.get_connection()
        for kj_file in files:
            kji = KassenjournalImporter(conn, kj_file)
            kji.load_file()
            queue.put(f"Datei '{kj_file}' geladen")
            kji.write_df()
            queue.put(f"Datei '{kj_file}' geschrieben")
            kji.update_table()
            queue.put(f"Datei '{kj_file}' in Zieltab")
        conn.close()
