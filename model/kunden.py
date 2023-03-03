import pandas as pd
import numpy as np
from sqlalchemy import Connection, Table, text
from datetime import datetime
from hashlib import md5

from model.db_manager import DbManager


class KundenImporter():
    '''Uebernimmt den Import der Kunden in die Datenbank'''

    def __init__(self, db_manager: DbManager, import_file: str) -> None:
        self.db_manager = db_manager
        self.import_file = import_file
        self._listeners = set()
        self.df: pd.DataFrame = None
        self.tab_temp: Table = None

    def write_data(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''
        self.tab_temp: Table = self.db_manager.tables['tab_kunden_temp']

        conn = self.db_manager.get_engine().connect()
        with conn:
            conn.execute(self.tab_temp.delete())

            self.df.to_sql(self.tab_temp.name, conn,
                           if_exists='append', index=False)
            conn.commit()
        conn.close()

    def load_file(self) -> None:
        '''
        Startet den Import der Kundendaten in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle(n) mittels ::update_table gestartet werden.
        '''
        ts = datetime.now()

        df_kdn = pd.read_csv(
            self.import_file, encoding='latin1', sep=';', decimal=',',
            usecols=['KDNR','Name', 'Rabattsatz']
        ).rename(
            columns={
                'KDNR': 'kdnr',
                'Name': 'kd_name',
                'Rabattsatz': 'rabatt_satz'
            }
        )
        df_kdn['kdnr'] = df_kdn['kdnr'].astype(str)
        df_kdn['rabatt_satz'] = df_kdn['rabatt_satz'].astype(np.float64)
        df_kdn['eintrag_ts'] = pd.to_datetime(ts)
        df_kdn['quelle'] = 'scs_export'
        df_kdn['hash'] = df_kdn['kdnr'].astype(str).apply(lambda s: md5(s.encode('utf-8')).hexdigest())
        df_kdn['hash_diff'] = (df_kdn['kd_name'].astype(str) + ':' + df_kdn['rabatt_satz'].astype(str)).apply(lambda s: md5(s.encode('utf-8')).hexdigest())

        self.df = df_kdn

    def post_process(self) -> None:
        '''Nach der Beladung der Zwischentabelle wird mittels dieser Methode die Beladung der Zieltabelle gestartet.'''

        conn = self.db_manager.get_engine().connect()
        with conn:
            self._belade_hub(conn)
            self._update_zuletzt_gesehen(conn)
            self._loesche_ungueltige_sat(conn)
            self._fuege_neue_sat_ein(conn)
            conn.commit()
        conn.close()

    def _belade_hub(self, conn: Connection) -> None:
        '''belaedt erstmal den HUB'''

        sql = '''
        INSERT INTO hub_kunden_t (hash, eintrag_ts, zuletzt_gesehen, quelle, kdnr)
        SELECT
            t.hash,
            t.eintrag_ts,
            t.eintrag_ts AS zuletzt_gesehen,
            t.quelle,
            t.kdnr

        FROM temp_kunden_t AS t

        LEFT JOIN hub_kunden_t AS h
            ON	t.hash = h.hash

        WHERE h.hash IS NULL

        '''

        conn.execute(text(sql))

    def _loesche_ungueltige_sat(self, conn: Connection) -> None:
        '''
        Setzt Eintraege in 'sat_kunden_t' ungueltig, fuer die aktualisierte Eintraege
        vorhanden sind. Bestehende Eintraege, die nicht in der Eingabedatei vorkommen, bleiben
        unberuehrt.
        '''
        sql = '''
        UPDATE sat_kunden_t
        SET 
            gueltig_bis = datetime('now', 'localtime'),
            gueltig = 0

        WHERE hash IN (
            SELECT 
                s.hash
                
            FROM sat_kunden_t as s

            LEFT JOIN temp_kunden_t AS t
                ON	t.hash = s.hash

            WHERE	s.gueltig = 1
            AND 	t.hash_diff <> s.hash_diff
        )
        '''
        conn.execute(text(sql))

    def _fuege_neue_sat_ein(self, conn: Connection) -> None:
        '''
        Fuegt neue Eintraege aus in 'sat_kunden_t' ein bzw. Eintraege, fuer die aktuellere
        Daten vorhanden sind.
        '''
        sql = '''
        INSERT INTO sat_kunden_t (hash, hash_diff, eintrag_ts, gueltig_bis, gueltig, quelle, kd_name, rabatt_satz)
        SELECT 
            t.hash,
            t.hash_diff,
            t.eintrag_ts,
            datetime('2099-12-31 23:59:59.000000') as gueltig_bis,
            1 as gueltig,
            t.quelle,
            t.kd_name,
            t.rabatt_satz
            
        FROM temp_kunden_t as t

        LEFT JOIN sat_kunden_t AS s
            ON	t.hash = s.hash
            AND s.gueltig = 1

        WHERE s.hash IS NULL
        '''
        conn.execute(text(sql))

    def _update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        sql = '''
        UPDATE hub_kunden_t
        SET zuletzt_gesehen = bas.eintrag_ts
        FROM (
            SELECT
                t.eintrag_ts,
                t.hash,
                t.kdnr

            FROM temp_kunden_t AS t
            JOIN hub_kunden_t AS h
                ON h.hash = t.hash
        ) AS bas
        WHERE bas.hash = hub_kunden_t.hash
        '''
        conn.execute(text(sql))


class KundenStatus():
    '''Holt Informationen zu den gespeicherten Kundendaten'''

    def __init__(self, db_manager: DbManager) -> None:
        super().__init__()
        self.db_manager = db_manager

    @property
    def letzte_aenderung(self) -> datetime:
        '''
        Ermittelt den letzten Import in der Datenbank.
        Dazu wird das neueste 'zuletzt_gesehen'-Datum ermittelt
        '''
        SQL = """
        SELECT MAX(h.zuletzt_gesehen) AS zeitpunkt FROM hub_kunden_t AS h
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        conn.close()
