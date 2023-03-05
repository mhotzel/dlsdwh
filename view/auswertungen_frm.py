from ttkbootstrap import Frame, LabelFrame, Label, Button, Entry, Separator

from controller.controller import Controller


class AuswertungenFrame(Frame):
    '''Implementiert einen Frame, der die Funktionen zum Auswerten von Daten anbietet'''

    def __init__(self, master, application: Controller) -> None:
        super().__init__(master)

        self.controller = master
        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self._frm_auswertungen = LabelFrame(self, text='Auswertungskatalog')
        self._frm_auswertungen.pack(fill='both', expand=True)

        self._frm_auswertungen.columnconfigure(1, weight=1)

        self.lbl_auswahl_std = Label(self._frm_auswertungen, text='Standardreports', font=('', 0, 'bold'))
        self.lbl_auswahl_std.grid(row=1, column=0, padx=5, pady=(10, 5), sticky='W')
        
        self.btn_report_backwaren = Button(self._frm_auswertungen, text='Bestellhilfe Backwaren', bootstyle='secondary')
        self.btn_report_backwaren.grid(row=5, column=0, sticky='WE', padx=5, pady=5)
        
        self.btn_report_metzger_kurz = Button(self._frm_auswertungen, text='Bestellhilfe Wurst- und Fleisch Kurz', bootstyle='secondary')
        self.btn_report_metzger_kurz.grid(row=10, column=0, sticky='WE', padx=5, pady=5)

        self.sep_sonstiges = Separator(self._frm_auswertungen, orient='horizontal')
        self.sep_sonstiges.grid(row=101, column=0, columnspan=2, sticky='WE', padx=10, pady=10)

        self.lbl_auswahl_inventur = Label(self._frm_auswertungen, text='Jahresabschluss / Inventur', font=('', 0, 'bold'))
        self.lbl_auswahl_inventur.grid(row=102, column=0, padx=5, pady=(10, 5), sticky='W')

        self.btn_report_inventur_artikeldatei = Button(self._frm_auswertungen, text='Artikeldatei für Inventur', bootstyle='secondary')
        self.btn_report_inventur_artikeldatei.grid(row=110, column=0, sticky='WE', padx=5, pady=5)

        self.btn_report_inventur_lieferartikel = Button(self._frm_auswertungen, text='Lieferantenartikel für Inventurnachbearb.', bootstyle='secondary')
        self.btn_report_inventur_lieferartikel.grid(row=120, column=0, sticky='WE', padx=5, pady=5)

    def _register_bindings(self) -> None:
        '''registriert die Events'''
        pass

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
