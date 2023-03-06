from ttkbootstrap import Button, Entry, Frame, Label, LabelFrame, Separator

from controller.controller import Controller
from model.db_manager import DbManager


class ImportDlsListenFrame(Frame):
    '''Frame zum Import der DLS-eigenen Listen'''

    def __init__(self, master, application: Controller, db_man: DbManager) -> None:
        super().__init__(master)

        self.application = application
        self.db_manager = db_man
        self.controls = set()
        self._build_ui()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self._frm_import = LabelFrame(self, text='Import DLS-eigener Listen')
        self._frm_import.pack(fill='both', expand=True)

        self.lbl_auswahl_std = Label(self._frm_import, text='Importfunktionen', font=('', 0, 'bold'))
        self.lbl_auswahl_std.grid(row=1, column=0, padx=5, pady=(10, 5), sticky='W')

        self.btn_import_preisliste = Button(self._frm_import, text='Import DLS-Preislisten', bootstyle='secondary')
        self.btn_import_preisliste.grid(row=10, column=0, sticky='WE', padx=5, pady=5)

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
