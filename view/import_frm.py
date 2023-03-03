from datetime import date, datetime
from tkinter import END, StringVar
from tkinter.messagebox import showwarning

from ttkbootstrap import Button, Entry, Frame, Label, LabelFrame
from ttkbootstrap.scrolled import ScrolledText

from controller.controller import Controller
from controller.job_control import ImportJobController
from model.artikel import ArtikelImporter, ArtikelStatus
from model.db_manager import DbManager
from model.errors import DatenImportError
from model.kassenjournal import KassenjournalImporter, KassenjournalStatus
from model.kunden import KundenImporter, KundenStatus
from model.log_level import LogLevel
from model.pfand import PfandImporter, PfandStatus
from model.warengruppen import WarengruppenImporter, WarengruppenStatus


class ImportFrame(Frame):
    '''Implementiert einen Frame, der die Funktionen zum Import von Daten anbietet'''

    def __init__(self, master, application: Controller, db_man: DbManager) -> None:
        super().__init__(master)

        self.application = application
        self.db_manager = db_man
        self.controls = set()
        self._build_ui()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self._frm_import = LabelFrame(self, text='Importfunktionen')
        self._frm_import.pack(fill='both', expand=True)

        self.lbl_headline = Label(
            self._frm_import, text='Letzter Import:', bootstyle='primary')
        self.lbl_headline.grid(row=0, column=1, sticky='WE', padx=10, pady=10)

        self.btn_imp_kassenjournal = Button(
            self._frm_import, text='Kassenjournal importieren', bootstyle='secondary', command=self.import_kassenjournal)
        self.btn_imp_kassenjournal.grid(
            row=1, column=0, sticky='WE', padx=10, pady=10)

        self.letzter_imp_kassenjournal = StringVar(self._frm_import)
        self.fld_letzter_imp_kassenjournal = Entry(
            self._frm_import, state='readonly', width='20', textvariable=self.letzter_imp_kassenjournal)
        self.fld_letzter_imp_kassenjournal.grid(
            row=1, column=1, sticky='WE', padx=10, pady=10)

        self.btn_kj_infos = Button(
            self._frm_import, text='Status-Infos Kassenjournal ermitteln', bootstyle='secondary', command=self.schreibe_kj_status)
        self.btn_kj_infos.grid(row=1, column=2, sticky='WE', padx=10, pady=10)
        self.controls.add(self.btn_imp_kassenjournal)

        self.btn_import_warengruppen = Button(self._frm_import, text='Warengruppen importieren', bootstyle='secondary', command=self.import_warengruppen)
        self.btn_import_warengruppen.grid(row=2, column=0, sticky='WE', padx=10, pady=10)
        self.controls.add(self.btn_import_warengruppen)

        self.letzter_imp_warengruppen = StringVar(self._frm_import)
        self.fld_letzter_imp_warengruppen = Entry(self._frm_import, state='readonly', width=20, textvariable=self.letzter_imp_warengruppen)
        self.fld_letzter_imp_warengruppen.grid(row=2, column=1, sticky='WE', padx=10, pady=10)

        self.btn_import_kundendaten = Button(self._frm_import, text='Kundendaten importieren', bootstyle='secondary', command=self.import_kundendaten)
        self.btn_import_kundendaten.grid(row=3, column=0, sticky='WE', padx=10, pady=10)
        self.controls.add(self.btn_import_kundendaten)

        self.letzter_imp_kundendaten = StringVar(self._frm_import)
        self.fld_letzter_imp_kundendaten = Entry(self._frm_import, state='readonly', width=20, textvariable=self.letzter_imp_kundendaten)
        self.fld_letzter_imp_kundendaten.grid(row=3, column=1, sticky='WE', padx=10, pady=10)

        self.btn_import_scsartikel = Button(
            self._frm_import, text='Schapfl-Artikelliste importieren', bootstyle='secondary', command=self.import_artikeldaten)
        self.btn_import_scsartikel.grid(
            row=20, column=0, sticky='WE', padx=10, pady=10)
        self.controls.add(self.btn_import_scsartikel)

        self.letzter_imp_scs_artikel = StringVar(self._frm_import)
        self.fld_letzter_imp_scs_artikel = Entry(
            self._frm_import, state='readonly', width='20', textvariable=self.letzter_imp_scs_artikel)
        self.fld_letzter_imp_scs_artikel.grid(
            row=20, column=1, sticky='WE', padx=10, pady=10)
        
        self.btn_import_pfanddaten = Button(self._frm_import, text='Pfanddaten importieren', bootstyle='secondary', command=self.import_pfanddaten)
        self.btn_import_pfanddaten.grid(row=21, column=0, sticky='WE', padx=10, pady=10)
        self.controls.add(self.btn_import_pfanddaten)

        self.letzter_imp_pfanddaten = StringVar(self._frm_import)
        self.fld_letzter_imp_pfanddaten = Entry(self._frm_import, state='readonly', width=20, textvariable=self.letzter_imp_pfanddaten)
        self.fld_letzter_imp_pfanddaten.grid(row=21, column=1, sticky='WE', padx=10, pady=10)

        # --------------------------

        self._frm_status = LabelFrame(self, text='Status-Infos')
        self._frm_status.pack(fill='both', expand=True)
        self._frm_status.columnconfigure(1, weight=1)

        self.fld_msg = ScrolledText(
            self._frm_status, height=10, width=120, state='disabled')
        self.fld_msg.grid(row=1, column=0, columnspan=2,
                          sticky='WE', padx=10, pady=10, ipadx=10, ipady=10)

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
        self.update_letzter_import_kj()
        self.update_letzter_import_wgr()
        self.update_letzter_import_kdn()
        self.update_letzter_import_artikel()
        self.update_letzter_import_pfand()

    def log_message(self, msg: str) -> None:
        '''Fuegt dem Nachrichten einen neuen Eintrag hinzu'''
        self.fld_msg.text.configure(state='normal')
        self.fld_msg.text.insert(END, msg + '\n')
        self.fld_msg.text.configure(state='disabled')

    def done(self) -> None:
        '''Wird aufgerufen vom ImportJob, wenn die Verarbeitung abgeschlossen ist'''
        self.update_letzter_import_kj()
        self.update_letzter_import_wgr()
        self.update_letzter_import_kdn()
        self.update_letzter_import_artikel()
        self.update_letzter_import_pfand()

        for ctrl in self.controls:
            ctrl.configure(state='normal')

    def import_kassenjournal(self) -> None:
        '''Importiert das Kassenjournal'''

        # self.btn_imp_kassenjournal.configure(state='disabled')
        for ctrl in self.controls:
            ctrl.configure(state='disabled')

        try:
            job_controller = ImportJobController(
                self.application, self, KassenjournalImporter, db_manager=self.db_manager)

            job_controller.importfile_ermitteln(
                defaultextension='SCHAPFL-Kassenjournaldatei (*.csv)',
                filetypes=[
                    ('Kassenjournal-Datei', '*.csv')
                ])

            job_controller.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import der Kassenjournale',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
            self.done()
    
    def import_warengruppen(self) -> None:
        '''Importiert die Warengruppen'''

        # self.btn_imp_kassenjournal.configure(state='disabled')
        for ctrl in self.controls:
            ctrl.configure(state='disabled')

        try:
            job_controller = ImportJobController(
                self.application, self, WarengruppenImporter, db_manager=self.db_manager)

            job_controller.importfile_ermitteln(
                defaultextension='SCHAPFL-Warengruppendatei (*.txt)',
                filetypes=[
                    ('Warengruppen-Datei', '*.txt')
                ])

            job_controller.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import der Warengruppendaten',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
            self.done()

    def import_kundendaten(self) -> None:
        '''Importiert die Kundendaten'''

        for ctrl in self.controls:
            ctrl.configure(state='disabled')

        try:
            job_controller = ImportJobController(
                self.application, self, KundenImporter, db_manager=self.db_manager)

            job_controller.importfile_ermitteln(
                defaultextension='SCHAPFL-Kundendatei (*.txt)',
                filetypes=[
                    ('Kundendaten-Datei', '*.txt')
                ])

            job_controller.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import der Kundendaten',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
            self.done()

    def import_artikeldaten(self) -> None:
        '''Importiert die Artikeldaten'''

        for ctrl in self.controls:
            ctrl.configure(state='disabled')

        try:
            job_controller = ImportJobController(
                self.application, self, ArtikelImporter, db_manager=self.db_manager)

            job_controller.importfile_ermitteln(
                defaultextension='SCHAPFL-Artikeldatei (*.txt)',
                filetypes=[
                    ('Artikel-Datei', '*.txt')
                ])

            job_controller.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import der Artikeldaten',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
            self.done()

    def import_pfanddaten(self) -> None:
        '''Importiert die Pfanddaten'''

        for ctrl in self.controls:
            ctrl.configure(state='disabled')

        try:
            job_controller = ImportJobController(
                self.application, self, PfandImporter, db_manager=self.db_manager)

            job_controller.importfile_ermitteln(
                defaultextension='SCHAPFL-Pfanddatei (*.txt)',
                filetypes=[
                    ('Pfand-Datei', '*.txt')
                ])

            job_controller.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import der Pfanddaten',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
            self.done()

    def update_letzter_import_kj(self) -> None:
        '''ermittelt und setzt das Datum den letzten Imports des Kassenjournals'''

        letzte_aenderung = KassenjournalStatus(
            self.application.db_manager).letzte_aenderung
        if letzte_aenderung:
            self.letzter_imp_kassenjournal.set(
                letzte_aenderung.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            self.letzter_imp_kassenjournal.set('Kein Import vorhanden')

    def update_letzter_import_wgr(self) -> None:
        '''ermittelt und setzt das Datum den letzten Imports der Warengruppen'''

        letzte_aenderung = WarengruppenStatus(
            self.application.db_manager).letzte_aenderung
        if letzte_aenderung:
            self.letzter_imp_warengruppen.set(
                letzte_aenderung.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            self.letzter_imp_warengruppen.set('Kein Import vorhanden')

    def update_letzter_import_kdn(self) -> None:
        '''ermittelt und setzt das Datum den letzten Imports der Kundendaten'''

        letzte_aenderung = KundenStatus(
            self.application.db_manager).letzte_aenderung
        if letzte_aenderung:
            self.letzter_imp_kundendaten.set(
                letzte_aenderung.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            self.letzter_imp_kundendaten.set('Kein Import vorhanden')

    def update_letzter_import_artikel(self) -> None:
        '''ermittelt und setzt das Datum den letzten Imports der Kundendaten'''

        letzte_aenderung = ArtikelStatus(
            self.application.db_manager).letzte_aenderung
        if letzte_aenderung:
            self.letzter_imp_scs_artikel.set(
                letzte_aenderung.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            self.letzter_imp_scs_artikel.set('Kein Import vorhanden')

    def update_letzter_import_pfand(self) -> None:
        '''ermittelt und setzt das Datum den letzten Imports der Pfanddaten'''

        letzte_aenderung = PfandStatus(
            self.application.db_manager).letzte_aenderung
        if letzte_aenderung:
            self.letzter_imp_pfanddaten.set(
                letzte_aenderung.strftime('%d.%m.%Y %H:%M:%S'))
        else:
            self.letzter_imp_pfanddaten.set('Kein Import vorhanden')

    def schreibe_kj_status(self) -> None:
        '''ermittelt und schreibt Status-Infos zum Kassenjournal in das Statusfeld'''

        monatsliste = KassenjournalStatus(self.application.db_manager).monate
        self.fld_msg.text.configure(state='normal')

        self.fld_msg.text.insert(
            END, f"{datetime.now().strftime('%d.%m.%Y %H:%M:%S')} - Kassenjournal - gespeicherte Monate:\n")
        if monatsliste:
            for monat in monatsliste:
                self.fld_msg.text.insert(END, f'  {monat}\n')
        else:
            self.fld_msg.text.insert(
                END, f'  Keine Kassenjournaldaten vorhanden\n')
        self.fld_msg.text.insert(END, f'\n')
        self.fld_msg.text.configure(state='disabled')
