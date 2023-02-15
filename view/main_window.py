from configparser import ConfigParser
from pathlib import Path
from tkinter import StringVar

from ttkbootstrap import Label, Separator, Window

from model.router import Router, RoutingEvent
from settings import IMGDIR
from view.auswertungen_frm import AuswertungenFrame
from view.button_bar import ButtonBar
from view.import_frm import ImportFrame
from view.start_frm import StartFrame


class MainWindow(Window):

    def __init__(self, cfg: ConfigParser):
        super().__init__(
            title='DLS Warenwirtschaft',
            themename='litera',
            #size=(700, 500),
            iconphoto=Path(IMGDIR) / 'logo.png'
        )

        self.cfg_parser = cfg
        dbfile = cfg['datenbank']['dbfile']
        self.controller = Router(self, dbfile)
        self._build_ui()
        self._register_bindings()

        self.set_status_message(f"Verwendete Datenbank: '{dbfile}'")

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''
        self.rowconfigure(0, weight=1)
        self.columnconfigure(2, weight=1)

        self.button_bar = ButtonBar(self, self.controller)
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
            StartFrame: StartFrame(self, self.controller),
            ImportFrame: ImportFrame(self, self.controller),
            AuswertungenFrame: AuswertungenFrame(self, self.controller)
        }

        for _, frm in self._frames.items():
            frm.grid(row=0, column=2, sticky='NSWE', pady=10, padx=10)

        self._frames[StartFrame].show()
        self.position_center()

    def _register_bindings(self) -> None:
        '''registriert Event Listeners usw'''
        self.controller.register_listener(
            RoutingEvent.EVT_ZEIGE_IMPORT_FRAME, lambda: self._frames[ImportFrame].show())

        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        self.controller.register_listener(
            RoutingEvent.EVT_ZEIGE_AUSWERTUNGEN_FRAME, lambda: self._frames[AuswertungenFrame].show())

    def on_closing(self) -> None:
        '''Raumt auf und beendet'''
        self.destroy()
        self.controller.db_manager.close()

    def set_status_message(self, msg: str, millis: int = 10_000) -> None:
        '''Zeigt die Nachricht in der Statusbar für den angegebenen Zeitraum an'''
        self.status_bar_text.set(msg)
        self.after(millis, lambda: self.status_bar_text.set(''))
