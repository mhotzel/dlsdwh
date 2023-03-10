import pandas as pd
import numpy as np
from sqlalchemy import Connection, Table, text
from datetime import datetime, date
from hashlib import md5

from model.db_manager import DbManager, concat


class PresseArtikelImporter():
    '''Uebernimmt den Import der Presseartikel in die Datenbank'''

    def __init__(self, db_manager: DbManager, import_file: str, export_date: date) -> None:
        self.db_manager = db_manager
        self.import_file = import_file
        self._listeners = set()
        self.df_artikel: pd.DataFrame = None
        self.df_liefart: pd.DataFrame = None
        self.tab_temp: Table = None
        self.export_date: date = export_date
        self.ts = datetime.now()

    def write_data(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''
        self.tab_temp_artikel: Table = self.db_manager.meta_data.tables['temp_artikel_t']
        self.tab_temp_liefart: Table = self.db_manager.meta_data.tables['temp_scs_liefart_t']

        conn = self.db_manager.get_engine().connect()
        with conn:
            conn.execute(self.tab_temp_artikel.delete())
            conn.execute(self.tab_temp_liefart.delete())

            self.df_artikel.to_sql(self.tab_temp_artikel.name, conn,
                                   if_exists='append', index=False)

            self.df_liefart.to_sql(self.tab_temp_liefart.name, conn,
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
            usecols=['PrStrichcode', 'Titel', 'EKPreis',
                     'VKPreis', 'MwStID', 'IsFSK'],
            dtype={
                'PrStrichcode': str, 'Titel': str, 'EKPreis': np.float64,
                'VKPreis': np.float64, 'MwStID': str, 'IsFSK': str
            }
        ).rename(
            columns={
                'PrStrichcode': 'art_nr', 'Titel': 'art_bez', 'EKPreis': 'ek_netto',
                'VKPreis': 'vk_brutto', 'MwStID': 'mwst_kz', 'IsFSK': 'fsk_kz'
            }
        )
        df['eintrag_ts'] = pd.to_datetime(self.ts)
        df['quelle'] = 'scs_export_presseartikel'
        df['export_datum'] = pd.to_datetime(self.export_date)

        self.df_artikel = self._lade_artikel(df)
        self.df_liefart = self._lade_liefart(df)

    def _lade_artikel(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Laedt die Presseartikel als SCS-Artikel.
        Dabei wird das eingelesene Presseartikel-DataFrame passend transformiert.
        '''
        df_artikel = df[['art_nr', 'art_bez', 'vk_brutto',
                         'mwst_kz', 'fsk_kz', 'eintrag_ts', 'quelle']].copy()
        df_artikel['idx'] = 0
        df_artikel['scs_pool_id'] = df_artikel['art_nr'].astype(np.int64)
        df_artikel['mengenfaktor'] = 1
        df_artikel['preiseinheit'] = 1
        df_artikel['preisgebunden_kz'] = 'J'
        df_artikel['rabatt_kz'] = 'N'
        df_artikel['wgr'] = ''
        df_artikel['wgr'] = np.where(
            df_artikel['mwst_kz'] == '9', '9999:0', df_artikel['wgr'])
        df_artikel['wgr'] = np.where(
            df_artikel['mwst_kz'] == '4', '9998:0', df_artikel['wgr'])
        df_artikel['wgr'] = np.where(
            df_artikel['mwst_kz'] == '0', '9997:0', df_artikel['wgr'])
        df_artikel['hash'] = df['art_nr'].apply(
            lambda s: md5(s.encode('utf-8')).hexdigest())
        df_artikel['hash_diff'] = concat(
            df_artikel[['art_bez', 'vk_brutto', 'fsk_kz',
                        'scs_pool_id', 'mengenfaktor', 'preiseinheit', 'wgr']]
        ).apply(lambda s: md5(s.encode('utf-8')).hexdigest())
        df_artikel = df_artikel.drop(columns=['mwst_kz'])

        return df_artikel.copy()

    def _lade_liefart(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Laedt die Presseartikel als SCS-Artikel.
        Dabei wird das eingelesene Presseartikel-DataFrame passend transformiert.
        '''
        df_liefart = df[['art_nr', 'ek_netto', 'eintrag_ts', 'quelle']].copy()
        df_liefart['lief_nr'] = '34'
        df_liefart['lief_art_nr'] = df_liefart['art_nr']
        df_liefart = df_liefart.rename(columns={'art_nr': 'ean'})

        df_liefart['hash'] = concat(df_liefart[['ean', 'lief_nr']]).apply(
            lambda s: md5(s.encode('utf-8')).hexdigest())
        df_liefart['hash_diff'] = concat(df_liefart[['lief_art_nr', 'ek_netto']]).apply(
            lambda s: md5(s.encode('utf-8')).hexdigest())

        return df_liefart.copy()

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

        self._artikel_belade_hub(conn)
        self._liefart_belade_hub(conn)

    def _loesche_ungueltige_sat(self, conn: Connection) -> None:
        '''
        Setzt Eintraege in 'sat_kunden_t' ungueltig, fuer die aktualisierte Eintraege
        vorhanden sind. Bestehende Eintraege, die nicht in der Eingabedatei vorkommen, bleiben
        unberuehrt.
        '''
        self._artikel_loesche_ungueltige_sat(conn)
        self._liefart_loesche_ungueltige_sat(conn)

    def _fuege_neue_sat_ein(self, conn: Connection) -> None:
        '''
        Fuegt neue Eintraege aus in 'sat_kunden_t' ein bzw. Eintraege, fuer die aktuellere
        Daten vorhanden sind.
        '''
        self._artikel_fuege_neue_sat_ein(conn)
        self._liefart_fuege_neue_sat_ein(conn)

    def _update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        self._artikel_update_zuletzt_gesehen(conn)
        self._liefart_update_zuletzt_gesehen(conn)

    def _artikel_belade_hub(self, conn: Connection) -> None:
        '''belaedt erstmal den HUB'''

        sql = '''
        INSERT INTO hub_artikel_t (hash, eintrag_ats, gueltig_adtm, zuletzt_gesehen, quelle, art_nr)
        SELECT
            t.hash,
            t.eintrag_ts,
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

    def _artikel_loesche_ungueltige_sat(self, conn: Connection) -> None:
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

    def _artikel_fuege_neue_sat_ein(self, conn: Connection) -> None:
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
            datetime('2099-12-31 23:59:59.000000') AS eintrag_ets,
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

    def _artikel_update_zuletzt_gesehen(self, conn: Connection) -> None:
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

    def _liefart_belade_hub(self, conn: Connection) -> None:
        '''belaedt erstmal den HUB'''

        sql = '''
        INSERT INTO hub_scs_liefart_t (hash, eintrag_ts, zuletzt_gesehen, quelle, ean, lief_nr)
        SELECT
            t.hash,
            t.eintrag_ts,
            t.eintrag_ts AS zuletzt_gesehen,
            t.quelle,
            t.ean,
            t.lief_nr

        FROM temp_scs_liefart_t AS t

        LEFT JOIN hub_scs_liefart_t AS h
            ON	t.hash = h.hash

        WHERE h.hash IS NULL
        '''

        conn.execute(text(sql))

    def _liefart_loesche_ungueltige_sat(self, conn: Connection) -> None:
        '''
        Setzt Eintraege ungueltig, fuer die aktualisierte Eintraege
        vorhanden sind. Bestehende Eintraege, die nicht in der Eingabedatei vorkommen, bleiben
        unberuehrt.
        '''
        sql = '''
        UPDATE sat_scs_liefart_t
        SET 
            gueltig_bis = datetime('now', 'localtime'),
            gueltig = 0

        WHERE hash IN (
            SELECT 
                s.hash
                
            FROM sat_scs_liefart_t as s

            LEFT JOIN temp_scs_liefart_t AS t
                ON	t.hash = s.hash

            WHERE	s.gueltig = 1
            AND 	t.hash_diff <> s.hash_diff
        )
        '''
        conn.execute(text(sql))

    def _liefart_fuege_neue_sat_ein(self, conn: Connection) -> None:
        '''
        Fuegt neue Eintraege aus der temp. Tabelle ein bzw. Eintraege, fuer die aktuellere
        Daten vorhanden sind.
        '''
        sql = '''
        INSERT INTO sat_scs_liefart_t 
        (hash, hash_diff, eintrag_ts, gueltig_bis, gueltig, quelle, lief_art_nr, ek_netto)
        SELECT 
            t.hash,
            t.hash_diff,
            t.eintrag_ts,
            datetime('2099-12-31 23:59:59.000000') as gueltig_bis,
            1 as gueltig,
            t.quelle,
            t.lief_art_nr,
            t.ek_netto

        FROM temp_scs_liefart_t as t

        LEFT JOIN sat_scs_liefart_t AS s
            ON	t.hash = s.hash
            AND s.gueltig = 1

        WHERE s.hash IS NULL
        '''
        conn.execute(text(sql))

    def _liefart_update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        sql = '''
        UPDATE hub_scs_liefart_t
        SET zuletzt_gesehen = bas.eintrag_ts
        FROM (
            SELECT
                t.eintrag_ts,
                t.hash

            FROM temp_scs_liefart_t AS t
            JOIN hub_scs_liefart_t AS h
                ON h.hash = t.hash
        ) AS bas
        WHERE bas.hash = hub_scs_liefart_t.hash
        '''
        conn.execute(text(sql))


class PresseArtikelStatus():
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
        SELECT MAX(h.zuletzt_gesehen) AS zeitpunkt FROM hub_artikel_t AS h WHERE h.quelle = 'scs_export_presseartikel'
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        conn.close()
