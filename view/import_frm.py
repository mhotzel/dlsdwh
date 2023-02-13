from ttkbootstrap import Frame, LabelFrame, Button, Label, Entry
from model.controller import Controller


class ImportFrame(Frame):
    '''Implementiert einen Frame, der die Funktionen zum Import von Daten anbietet'''

    def __init__(self, master, controller: Controller) -> None:
        super().__init__(master)

        self.controller = controller
        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self._frm_import = LabelFrame(self, text='Importfunktionen')
        self._frm_import.pack(fill='both', expand=True)

        self._frm_import.columnconfigure(0, weight=1)
        self._frm_import.columnconfigure(1, weight=1)

        self.lbl_headline = Label(
            self._frm_import, text='Letzter Import:', bootstyle='primary')
        self.lbl_headline.grid(row=0, column=1, sticky='WE', padx=10, pady=10)

        self.btn_imp_kassenjournal = Button(
            self._frm_import, text='Kassenjournal', bootstyle='secondary')
        self.btn_imp_kassenjournal.grid(
            row=1, column=0, sticky='WE', padx=10, pady=10)

        self.fld_letzter_imp_kassenjournal = Entry(
            self._frm_import, state='readonly', width='20')
        self.fld_letzter_imp_kassenjournal.grid(
            row=1, column=1, sticky='WE', padx=10, pady=10)

    def _register_bindings(self) -> None:
        '''registriert die Events'''
        self.btn_imp_kassenjournal.configure(
            command=lambda: self.controller.import_kassenjournal())

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
