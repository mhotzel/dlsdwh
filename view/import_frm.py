from tkinter import END
from ttkbootstrap import Frame, LabelFrame, Button, Label, Entry
from ttkbootstrap.scrolled import ScrolledText
from controller.controller import Controller
from model.errors import DatenImportError
from tkinter.messagebox import showwarning


from model.job_ctrl_kassenjournal import KjImportJobController
from model.log_level import LogLevel


class ImportFrame(Frame):
    '''Implementiert einen Frame, der die Funktionen zum Import von Daten anbietet'''

    def __init__(self, master, application: Controller) -> None:
        super().__init__(master)

        self.application = application
        self._build_ui()
        self._register_bindings()

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

        self.fld_letzter_imp_kassenjournal = Entry(
            self._frm_import, state='readonly', width='20')
        self.fld_letzter_imp_kassenjournal.grid(
            row=1, column=1, sticky='WE', padx=10, pady=10)

        self._frm_status = LabelFrame(self, text='Importstatus')
        self._frm_status.pack(fill='both', expand=True)
        self._frm_status.columnconfigure(1, weight=1)

        self.fld_msg = ScrolledText(
            self._frm_status, height=10, width=120, state='disabled')
        self.fld_msg.grid(row=1, column=0, columnspan=2,
                          sticky='WE', padx=10, pady=10, ipadx=10, ipady=10)

    def _register_bindings(self) -> None:
        '''registriert die Events'''

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()

    def log_message(self, msg: str) -> None:
        '''Fuegt dem Nachrichten einen neuen Eintrag hinzu'''
        self.fld_msg.text.configure(state='normal')
        self.fld_msg.text.insert(END, msg + '\n')
        self.fld_msg.text.configure(state='disabled')

    def import_kassenjournal(self) -> None:
        '''Importiert das Kassenjournal'''

        try:
            kj_job_ctrl = KjImportJobController(self.application)
            kj_job_ctrl.importfile_ermitteln()
            kj_job_ctrl.starte_import()

        except DatenImportError as de:
            showwarning(
                title='Fehler beim Import',
                message=de.args[0]
            )
            self.application.log_message(LogLevel.WARN, de.args[0])
