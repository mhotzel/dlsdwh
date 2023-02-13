from ttkbootstrap import Frame, LabelFrame, Label
from model.controller import Controller


class AuswertungenFrame(Frame):
    '''Implementiert einen Frame, der die Funktionen zum Auswerten von Daten anbietet'''

    def __init__(self, master, controller: Controller) -> None:
        super().__init__(master)

        self.controller = controller
        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self.columnconfigure(0, weight=1)
        self.columnconfigure(2, weight=1)

        self.lbl_title = Label(self, text="Auswertungen")
        self.lbl_title.grid(row=1, column=1, sticky='WE')

    def _register_bindings(self) -> None:
        '''registriert die Events'''
        pass

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
