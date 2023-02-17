
class DatenImportError(Exception):
    '''Steht für Fehler beim Datenimport'''

    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    