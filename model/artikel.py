import pandas as pd
import numpy as np
from sqlalchemy import Connection, Table, text
from datetime import datetime, date
from hashlib import md5

from model.db_manager import DbManager, concat

class ArtikelImporter():
    '''Uebernimmt den Import der Kassenartikel in die Datenbank'''

    def __init__(self, db_manager: DbManager, import_file: str, export_date: date) -> None:
        self.db_manager = db_manager
        self.import_file = import_file
        self._listeners = set()
        self.df: pd.DataFrame = None
        self.tab_temp: Table = None
        self.export_date: date = export_date
        self.ts = datetime.now()

    def write_data(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''
        self.tab_temp: Table = self.db_manager.meta_data.tables['temp_artikel_t']

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
        df = pd.read_csv(
            self.import_file, encoding='cp1252', sep=';', decimal=',',
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
                    'fsk_kz', 'notizen']]).astype(str).apply(lambda s: md5(s.encode('utf-8')).hexdigest())

        df['quelle'] = 'scs_export_artikel'
        df['eintrag_ts'] = pd.to_datetime(self.ts)
        df['export_datum'] = pd.to_datetime(self.export_date)
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
        INSERT INTO hub_artikel_t (hash, eintrag_ats, gueltig_adtm, zuletzt_gesehen, quelle, art_nr)
        SELECT
            t.hash,
            t.eintrag_ts AS eintrag_ats,
            t.export_datum AS gueltig_adtm,
            t.export_datum AS zuletzt_gesehen,
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
            eintrag_ets = :gueltig_ets,
            gueltig_edtm = :gueltig_edtm,
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
        conn.execute(text(sql), {'gueltig_ets': self.ts, 'gueltig_edtm': self.export_date})

    def _fuege_neue_sat_ein(self, conn: Connection) -> None:
        '''
        Fuegt neue Eintraege aus in 'sat_kunden_t' ein bzw. Eintraege, fuer die aktuellere
        Daten vorhanden sind.
        '''
        sql = '''
        INSERT INTO sat_artikel_t 
        (hash, hash_diff, eintrag_ats, eintrag_ets, gueltig_adtm, gueltig_edtm, gueltig, quelle, idx, scs_pool_id, art_bez, mengenfaktor, vk_brutto, preiseinheit, kurzcode, bontext, mengeneinheit, mengentyp, gpfaktor, wgr, rabatt_kz, preisgebunden_kz, fsk_kz, notizen)
        SELECT 
            t.hash,
            t.hash_diff,
            t.eintrag_ts AS eintrag_ats,
            datetime('2099-12-31 23:59:59.999999') AS eintrag_ets,
            :gueltig_adtm AS gueltig_adtm,
            date('2099-12-31') AS gueltig_edtm,
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
        conn.execute(text(sql), {'gueltig_adtm': self.export_date})

    def _update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        sql = '''
        UPDATE hub_artikel_t
        SET zuletzt_gesehen = bas.export_datum
        FROM (
            SELECT
                t.export_datum,
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
    def letzte_datei(self) -> date:
        '''
        Ermittelt das Datum der Datei mit dem jüngsten Import in der Datenbank.
        Dazu wird das neueste 'zuletzt_gesehen'-Datum ermittelt
        '''
        SQL = """
        SELECT MAX(h.zuletzt_gesehen) AS zeitpunkt FROM hub_artikel_t AS h WHERE h.quelle = 'scs_export_artikel'
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        conn.close()
