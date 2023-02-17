from tkinter.filedialog import askopenfilename

from model.errors import DatenImportError


class KjImportJobController():
    '''Job Controller für den Import des Kassenjournals'''

    def importfile_ermitteln(self) -> None:
        '''Ermittelt die zu importierende Datei'''

        self.filename = askopenfilename(
            title='Kassenjournal-Datei auswählen',
            defaultextension='SCHAPFL-Kassenjournaldatei (*.csv)',
            filetypes=[
                ('Kassenjournal-Datei', '*.csv')
            ]
        )

        if not self.filename:
            raise DatenImportError(f'Es wurde keine Eingabedatei gewählt')
