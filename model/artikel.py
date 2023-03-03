import pandas as pd
import numpy as np
from sqlalchemy import Connection, Table, text
from datetime import datetime
from hashlib import md5

from model.db_manager import DbManager

def concat(df: pd.DataFrame):
    res = None
    for i, col in enumerate(df.columns):
        if i == 0:
            res = df[col].astype(str)
        else:
            res += ':' + df[col].astype(str)
    return res

class ArtikelImporter():
    '''Uebernimmt den Import der Kassenartikel in die Datenbank'''

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
        self.tab_temp: Table = self.db_manager.tables['tab_artikel_temp']

        conn = self.db_manager.get_engine().connect()
        with conn:
            conn.execute(self.tab_temp.delete())

            self.df.to_sql(self.tab_temp.name, conn,
                           if_exists='append', index=False)
            conn.commit()
        conn.close()

    def load_file(self) -> None:
        '''
        Startet den Import der Kassenartikel in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle(n) mittels ::update_table gestartet werden.
        '''
        ts = datetime.now()

        df = pd.read_csv(
            self.import_file, encoding='latin1', sep=';', decimal=',',
            usecols=[
                'SCSPoolID', 'Strichcode', 'Index', 'Bezeichnung', 'Mengenfaktor', 'VKPreis', 'Preiseinheit',
                'Kurzcode', 'Bontext', 'Mengeneinheit', 'Mengentyp', 'GPFaktor', 'WGR', 'UWGR', 'RabattKZ', 
                'PreisgebundenKZ', 'FSKKZ', 'Notizen'
            ],
            dtype={
                'Strichcode': str, 'Mengenfaktor': np.float64, 'VKPreis': np.float64,
                'Preiseinheit': np.int64, 'Kurzcode': str, 'Bontext': str, 'Mengeneinheit': str,
                'Mengentyp': str, 'GPFaktor': np.float64, 'WGR': str, 'UWGR': str,
                'RabattKZ': str, 'PreisgebundenKZ': str, 'FSKKZ': str, 'Notizen': str
            }
        ).rename(
            columns={
                'SCSPoolID': 'scs_pool_id', 'Strichcode': 'art_nr', 'Index': 'idx', 'Bezeichnung': 'art_bez',
                'Mengenfaktor': 'mengenfaktor', 'VKPreis': 'vk_brutto', 'Preiseinheit': 'preiseinheit',
                'Kurzcode': 'kurzcode', 'Bontext': 'bontext', 'Mengeneinheit': 'mengeneinheit',
                'Mengentyp': 'mengentyp', 'GPFaktor': 'gpfaktor', 'WGR': 'wgr', 'UWGR': 'uwgr',
                'RabattKZ': 'rabatt_kz', 'PreisgebundenKZ': 'preisgebunden_kz', 'FSKKZ': 'fsk_kz',
                'Notizen': 'notizen'
            }
        )
        df['wgr'] = df['wgr'].str.cat(df['uwgr'], ':')
        df = df.drop(columns=['uwgr'])
        df['hash'] = df['art_nr'].apply(lambda data: md5(data.encode('utf-8')).hexdigest())

        df['hash_diff'] = concat(df[['scs_pool_id', 'idx', 'art_bez', 'mengenfaktor', 'vk_brutto', 'preiseinheit', 'kurzcode',
                    'bontext', 'mengeneinheit', 'mengentyp', 'gpfaktor', 'wgr', 'rabatt_kz', 'preisgebunden_kz',
                    'fsk_kz', 'notizen']])

        df['hash_diff'].to_csv('ausgabe.csv', sep=';', index=False)
        df['quelle'] = 'scs_export'
        df['eintrag_ts'] = pd.to_datetime(ts)
        df['preiseinheit'] = np.where(df['preiseinheit'] == 0, 1, df['preiseinheit'])

        self.df = df

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
        INSERT INTO hub_artikel_t (hash, eintrag_ts, zuletzt_gesehen, quelle, art_nr)
        SELECT
            t.hash,
            t.eintrag_ts,
            t.eintrag_ts AS zuletzt_gesehen,
            t.quelle,
            t.art_nr

        FROM temp_artikel_t AS t

        LEFT JOIN hub_artikel_t AS h
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
        UPDATE sat_artikel_t
        SET 
            gueltig_bis = datetime('now', 'localtime'),
            gueltig = 0

        WHERE hash IN (
            SELECT 
                s.hash
                
            FROM sat_artikel_t as s

            LEFT JOIN temp_artikel_t AS t
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
        INSERT INTO sat_artikel_t 
        (hash, hash_diff, eintrag_ts, gueltig_bis, gueltig, quelle, idx, scs_pool_id, art_bez, mengenfaktor, vk_brutto, preiseinheit, kurzcode, bontext, mengeneinheit, mengentyp, gpfaktor, wgr, rabatt_kz, preisgebunden_kz, fsk_kz, notizen)
        SELECT 
            t.hash,
            t.hash_diff,
            t.eintrag_ts,
            datetime('2099-12-31 23:59:59.000000') as gueltig_bis,
            1 as gueltig,
            t.quelle,
            t.idx,
            t.scs_pool_id,
            t.art_bez,
            t.mengenfaktor,
            t.vk_brutto,
            t.preiseinheit,
            t.kurzcode,
            t.bontext,
            t.mengeneinheit,
            t.mengentyp,
            t.gpfaktor,
            t.wgr,
            t.rabatt_kz,
            t.preisgebunden_kz,
            t.fsk_kz,
            t.notizen
            
        FROM temp_artikel_t as t

        LEFT JOIN sat_artikel_t AS s
            ON	t.hash = s.hash
            AND s.gueltig = 1

        WHERE s.hash IS NULL
        '''
        conn.execute(text(sql))

    def _update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        sql = '''
        UPDATE hub_artikel_t
        SET zuletzt_gesehen = bas.eintrag_ts
        FROM (
            SELECT
                t.eintrag_ts,
                t.hash

            FROM temp_artikel_t AS t
            JOIN hub_artikel_t AS h
                ON h.hash = t.hash
        ) AS bas
        WHERE bas.hash = hub_artikel_t.hash
        '''
        conn.execute(text(sql))


class ArtikelStatus():
    '''Holt Informationen zu den gespeicherten Artikeldaten'''

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
        SELECT MAX(h.zuletzt_gesehen) AS zeitpunkt FROM hub_artikel_t AS h
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        conn.close()
