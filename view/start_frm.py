from ttkbootstrap import Frame, LabelFrame, Label
from model.controller import Controller


class StartFrame(Frame):

    def __init__(self, master, controller: Controller) -> None:
        super().__init__(master)

        self.controller = controller

        self._build_ui()
        self._register_bindings()

    def _build_ui(self) -> None:
        '''Baut die Oberflaeche'''

        self.columnconfigure(0, weight=1)
        self.columnconfigure(2, weight=1)
        
        self.lbl_titel = Label(self, text='Startseite', font=('Arial', 24), anchor='center')
        self.lbl_titel.grid(row=1, column=1, sticky='WE', pady=20)
        
        self.lbl_info = Label(self, text='Funktion an der linken Seite auswählen', anchor='center', font=('', 0, 'bold'))
        self.lbl_info.grid(row=2, column=1, sticky='WE', pady=20)

    def _register_bindings(self) -> None:
        'Registriert die Events'

    def show(self) -> None:
        '''Bringt den Frame in den Vordergrund'''
        self.tkraise()
