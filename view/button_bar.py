from ttkbootstrap import Frame, Button
from controller.controller import Controller


class ButtonBar(Frame):
    '''Implementiert die seitliche Buttonleiste mit den Hauptfunktionen'''

    def __init__(self, master, controller: Controller) -> None:
        super().__init__(master)

        self.controller: Controller = controller
        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self.btn_import_scs = Button(self, text='Import aus Kassensystem')
        self.btn_import_scs.pack(fill='x', padx=5, pady=5)

        self.btn_import_dlslisten = Button(self, text='Import aus DLS-Listen')
        self.btn_import_dlslisten.pack(fill='x', padx=5, pady=5)

        self.btn_auswertungen = Button(self, text='Auswertungen')
        self.btn_auswertungen.pack(fill='x', padx=5, pady=5)

        self.btn_datenbank_selektieren = Button(self, text='Datenbank wählen')
        self.btn_datenbank_selektieren.pack(
            fill='x', padx=5, pady=5, side='bottom')

    def _register_bindings(self):
        '''Registriert Listener usw'''
        self.btn_import_scs.configure(
            command=self.controller.zeige_import_frame)

        self.btn_import_dlslisten.configure(
            command=self.controller.zeige_importdlslisten_frame)

        self.btn_auswertungen.configure(
            command=self.controller.zeige_auswertungen_frame)

        self.btn_datenbank_selektieren.configure(
            command=self.controller.selektiere_datenbank)
