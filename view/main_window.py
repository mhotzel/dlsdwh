from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from tkinter import StringVar
from tkinter.messagebox import showerror
from typing import Mapping

from ttkbootstrap import Label, Separator, Window

from model.db_manager import DbManager
from model.log_level import LogLevel
from settings import IMGDIR, LOG_FILE
from view.auswertungen_frm import AuswertungenFrame
from view.button_bar import ButtonBar
from view.import_frm import ImportFrame
from view.start_frm import StartFrame


class MainWindow(Window):

    def __init__(self, cfg: Mapping[str, str]):
        super().__init__(
            title='DLS Warenwirtschaft',
            themename='litera',
            # size=(700, 500),
            iconphoto=Path(IMGDIR) / 'logo.png'
        )

        self.db_manager = DbManager(cfg['dbfile'])
        self.log_file = open(LOG_FILE, mode='at')

        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''
        self.rowconfigure(0, weight=1)
        self.columnconfigure(2, weight=1)

        self.button_bar = ButtonBar(self, self)
        self.button_bar.grid(row=0, column=0, padx=10, pady=10, sticky='WNS')

        self.buttonbar_sep = Separator(self, orient='vertical')
        self.buttonbar_sep.grid(row=0, column=1, sticky='NS')

        self.statusbar_sep = Separator(self, orient='horizontal')
        self.statusbar_sep.grid(row=1, column=0, columnspan=3, sticky='WE')
        self.status_bar_text = StringVar()
        self.status_bar = Label(
            self, textvariable=self.status_bar_text)
        self.status_bar.grid(row=2, column=0, columnspan=3,
                             ipadx=5, ipady=5, sticky='WE')

        self._frames = {
            StartFrame: StartFrame(self),
            ImportFrame: ImportFrame(self, application=self, db_man=self.db_manager),
            AuswertungenFrame: AuswertungenFrame(self, application=self)
        }

        for _, frm in self._frames.items():
            frm.grid(row=0, column=2, sticky='NSWE', pady=10, padx=10)

        self._frames[StartFrame].show()
        self.position_center()

    def _register_bindings(self) -> None:
        '''registriert Event Listeners usw'''
        self.protocol("WM_DELETE_WINDOW", self._on_closing)

    def _on_closing(self) -> None:
        '''Raumt auf und beendet'''
        self.destroy()
        self.log_file.close()

    def _set_status_message(self, msg: str, millis: int = 10_000) -> None:
        '''Zeigt die Nachricht in der Statusbar für den angegebenen Zeitraum an'''
        self.status_bar_text.set(msg)

    def log_message(self, level: LogLevel, message: str) -> None:
        '''Loggt die uebergebene Nachricht.'''

        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"{ts} - {level.name} - {message}"
        self._set_status_message(msg)
        self.log_file.write(msg + '\n')

        if level > LogLevel.INFO:
            showerror(title='Fehler beim Import', message=message)

    def zeige_import_frame(self) -> None:
        '''Zeigt den Import-Frame an.'''
        self._frames[ImportFrame].show()

    def zeige_auswertungen_frame(self) -> None:
        '''Zeigt den Auswertungen-Frame an.'''
        self._frames[AuswertungenFrame].show()
