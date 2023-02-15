import sqlite3
from pathlib import Path


class DbManager():
    '''Managed die Datenbankverbindung'''

    def __init__(self, dbfile: str) -> None:

        self.dbfile = dbfile
        self.conn: sqlite3.Connection = None

    def get_connection(self) -> sqlite3.Connection:
        '''
        prueft, ob eine offene Datenbankverbindung existiert.
        Wenn nicht, wird eine erzeugt und zurueck gegeben
        '''
        uri = Path(self.dbfile).as_uri()

        if not self.conn:
            self.conn = sqlite3.connect(f"{uri}?mode=rw", uri=True)

        return self.conn

    def close(self) -> None:
        if self.conn:
            self.conn.close()
