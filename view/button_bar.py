from ttkbootstrap import Frame, Button

from model.router import Router


class ButtonBar(Frame):
    '''Implementiert die seitliche Buttonleiste mit den Hauptfunktionen'''

    def __init__(self, master, router: Router) -> None:
        super().__init__(master)

        self.router = router
        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self.btn_importseite = Button(self, text='Importe')
        self.btn_importseite.pack(fill='x', padx=5, pady=5)

        self.btn_auswertungen = Button(self, text='Auswertungen')
        self.btn_auswertungen.pack(fill='x', padx=5, pady=5)

    def _register_bindings(self):
        '''Registriert Listener usw'''
        self.btn_importseite.configure(
            command=self.router.zeige_import_frame)

        self.btn_auswertungen.configure(command=self.router.zeige_auswertungen_frame)
