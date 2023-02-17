import sqlite3
from pathlib import Path


class DbManager():
    '''Managed die Datenbankverbindung'''

    def __init__(self, dbfile: str) -> None:
        self.dbfile = dbfile

    def get_connection(self) -> sqlite3.Connection:
        '''
        Erzeugt eine Datenbankverbindung.
        '''
        uri = Path(self.dbfile).as_uri()

        return sqlite3.connect(f"{uri}?mode=rw", uri=True)
