from datetime import date, datetime
from hashlib import md5
from typing import List

import pandas as pd
import numpy as np
from sqlalchemy import Engine, Table, join, select, text, Connection

from model.db_manager import DbManager


def date_parser(ds: str) -> datetime:
    return datetime.strptime(ds, '%d.%m.%Y %H:%M:%S')


class KassenjournalImporter():
    '''Uebernimmt den Import des Kassenjournals in die Datenbank'''

    def __init__(self, db_manager: DbManager, import_file: str, export_date: date) -> None:
        self.db_manager = db_manager
        self.import_file = import_file
        self._listeners = set()
        self.df: pd.DataFrame = None
        self.tab_kj: Table = None
        self.tab_kjt: Table = None
        self.export_date: date = export_date

    def write_data(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''

        self.tab_kj: Table = self.db_manager.meta_data.tables['kassenjournal_t']
        self.tab_kjt: Table = self.db_manager.meta_data.tables['temp_kassenjournal_t']
        self.tab_bons: Table = self.db_manager.meta_data.tables['kassenbons_t']
        self.tab_bons_temp: Table = self.db_manager.meta_data.tables['temp_kassenbons_t']
        self.tab_bon_pos: Table = self.db_manager.meta_data.tables['kassenbons_pos_t']
        self.tab_bon_pos_temp: Table = self.db_manager.meta_data.tables['temp_kassenbons_pos_t']

        conn = self.db_manager.get_engine().connect()
        with conn:
            conn.execute(self.tab_kjt.delete())
            conn.execute(self.tab_bons_temp.delete())
            conn.execute(self.tab_bon_pos_temp.delete())

            self.df.to_sql(self.tab_kjt.name, conn,
                           if_exists='append', index=False)
            conn.commit()
        conn.close()

    def load_file(self) -> None:
        '''
        Startet den Import des Kassenjournals in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle mittels ::update_table gestartet werden.
        '''

        ts = datetime.now()
        df = pd.read_csv(
            self.import_file, sep=';', decimal=',', encoding='utf8',
            usecols=['Kassen-Nr.', 'Bon-Nr.', 'Zeitpunkt', 'Beginn', 'Verkäufer', 'Kunden-Nr.', 'Bon-Summe', 'Typ', 'Artikelnummer',
                     'Bezeichnung', 'Warengruppe', 'MwSt.-Satz', 'Mengenfaktor', 'Menge', 'Preis', 'Gesamt', 'Infotext', 'Stornoreferenz', 'TSE-Info'],
            dtype={
                'Kassen-Nr.': int,
                'Bon-Nr.': int,
                'Verkäufer': str,
                'Bon-Summe': float,
                'Typ': str,
                'Artikelnummer': str,
                'Bezeichnung': str,
                'Warengruppe': str,
                'MwSt.-Satz': str,
                'Preis': float,
                'Gesamt': float,
                'Infotext': str,
                'Stornoreferenz': str,
                'TSE-Info': str
            },
        ).rename(columns={
            'Kassen-Nr.': 'kasse_nr',
            'Bon-Nr.': 'bon_nr',
            'Beginn': 'bon_beginn',
            'Zeitpunkt': 'bon_abschluss',
            'Verkäufer': 'ma',
            'Kunden-Nr.': 'kdnr',
            'Bon-Summe': 'bon_summe',
            'Typ': 'typ',
            'Artikelnummer': 'art_nr',
            'Bezeichnung': 'art_bez',
            'Warengruppe': 'warengruppe',
            'MwSt.-Satz': 'mwst_satz',
            'Mengenfaktor': 'mengenfaktor',
            'Menge': 'menge',
            'Preis': 'preis_einzel',
            'Gesamt': 'preis_gesamt',
            'Infotext': 'infotext',
            'Stornoreferenz': 'storno_ref',
            'TSE-Info': 'tse_info'
        })

        df['mengenfaktor'] = df.mengenfaktor.where(
            ~df.mengenfaktor.isna(), 1).astype(int)
        df['eintrag_ts'] = pd.to_datetime(ts)
        df['pos'] = df[['bon_nr']].groupby('bon_nr').cumcount()
        df['bon_beginn'] = df['bon_beginn'].where(
            ~df['bon_beginn'].isna(), df['bon_abschluss'])
        df['kdnr'] = df.kdnr.where(
            ~df.kdnr.isna(), 0).astype(int)

        df['bon_beginn'] = pd.to_datetime(
            df['bon_beginn'], format='%d.%m.%Y %H:%M:%S')
        df['bon_abschluss'] = pd.to_datetime(
            df['bon_abschluss'], format='%d.%m.%Y %H:%M:%S')
        df['hash'] = (df['kasse_nr'].astype(str) + ":" + df['bon_nr'].astype(str) + ":" +
                      df['pos'].astype(str)).apply(lambda data: md5(data.encode('utf-8')).hexdigest())
        df['bon_typ'] = df['typ'].str.split('|', expand=True)[0].str.strip()
        df['pos_typ'] = df['typ'].str.split('|', expand=True)[1].str.strip()

        self.df = df

    def post_process(self) -> None:
        '''Nach der Beladung der Zwischentabelle wird mittels dieser Methode die Beladung der Zieltabellen gestartet.'''
        conn = self.db_manager.get_engine().connect()
        with conn:
            self._fuelle_kassenjournal(conn)
            self._belade_bons_temp(conn)
            self._belade_bons(conn)
            self._belade_bon_pos_temp(conn)
            self._belade_bon_pos(conn)
            self._belade_kalender(conn)
            conn.commit()
        conn.close()

    def _fuelle_kassenjournal(self, conn: Connection) -> None:
        '''Zieltabelle der Kassenjournale fuellen.'''

        sql = '''
        INSERT INTO kassenjournal_t
        SELECT
            kjt.*
            
        FROM temp_kassenjournal_t AS kjt

        LEFT JOIN kassenjournal_t AS kj
            ON	kjt.kasse_nr = kj.kasse_nr
            AND kjt.bon_nr = kj.bon_nr

        WHERE kj.bon_nr IS NULL
        '''
        conn.execute(text(sql))

    def _belade_bons_temp(self, conn: Connection) -> None:
        '''Aus der Kassenjournal-Zwischentabelle wird die Kassenbons-Zwischentabelle befuellt'''

        sql = '''
        WITH bon_typ_count AS (
        SELECT
            kjt.kasse_nr,
            kjt.bon_nr,
            kjt.bon_typ,
            CASE WHEN kjt.bon_typ = 'RN'
                THEN -1
                ELSE 1
            END AS rang
            
        FROM
            temp_kassenjournal_t kjt
            
        GROUP BY
            kjt.kasse_nr,
            kjt.bon_nr,
            kjt.bon_typ
        )

        SELECT 
            kjt.eintrag_ts,
            btmax.kasse_nr,
            btmax.bon_nr,
            btc.bon_typ,
            kjt.bon_beginn,
            kjt.bon_abschluss,
            kjt.bon_summe,
            kjt.kdnr,
            kjt.tse_info,
            kjt.storno_ref

        FROM (
            SELECT 
                btc.kasse_nr,
                btc.bon_nr,
                MAX(btc.rang) as max_rang
                
            FROM
                bon_typ_count AS btc

            GROUP BY
                btc.kasse_nr,
                btc.bon_nr
        ) as btmax
        JOIN bon_typ_count as btc
            ON  btmax.kasse_nr = btc.kasse_nr
            AND btmax.bon_nr = btc.bon_nr
            AND btmax.max_rang = btc.rang

        JOIN (
            SELECT DISTINCT
                kjt.eintrag_ts,
                kjt.kasse_nr,
                kjt.bon_nr,
                kjt.bon_beginn,
                kjt.bon_abschluss,
                kjt.bon_summe,
                kjt.kdnr,
                kjt.tse_info,
                kjt.storno_ref
            FROM
                temp_kassenjournal_t AS kjt
        ) as kjt
            ON  btmax.kasse_nr = kjt.kasse_nr
            AND btmax.bon_nr = kjt.bon_nr
        '''
        df_bon_zwischen = pd.read_sql_query(text(sql), conn)
        df_bon_zwischen['hash'] = (df_bon_zwischen['kasse_nr'].astype(
            str) + ":" + df_bon_zwischen['bon_nr'].astype(str)).apply(lambda data: md5(data.encode('utf-8')).hexdigest())
        df_bon_zwischen['eintrag_ts'] = pd.to_datetime(
            df_bon_zwischen.eintrag_ts)
        df_bon_zwischen['bon_beginn'] = pd.to_datetime(
            df_bon_zwischen.bon_beginn)
        df_bon_zwischen['bon_abschluss'] = pd.to_datetime(
            df_bon_zwischen.bon_abschluss)
        df_bon_zwischen['bon_datum'] = pd.to_datetime(
            df_bon_zwischen.bon_abschluss).dt.date

        conn.execute(self.tab_bons_temp.delete())
        df_bon_zwischen.to_sql(self.tab_bons_temp.name,
                               conn, index=False, if_exists='append')

    def _belade_bons(self, conn: Connection) -> None:
        '''Aus der Kassenbons-Zwischentabelle wird die Kassenbons-Zieltabelle befuellt'''
        sql = '''
        INSERT INTO kassenbons_t
        SELECT
            bt.*
            
        FROM temp_kassenbons_t AS bt

        LEFT JOIN kassenbons_t AS b
            ON	bt.hash = b.hash

        WHERE b.hash IS NULL
        '''
        conn.execute(text(sql))

    def _belade_bon_pos_temp(self, conn: Connection) -> None:
        '''Aus der Kassenjournal-Zwischentabelle wird die Kassenpositionen-Zwischentabelle befuellt'''

        sql = 'SELECT kjt.* FROM temp_kassenjournal_t AS kjt ORDER BY kjt.kasse_nr, kjt.bon_nr, kjt.pos'
        df = pd.read_sql_query(text(sql), con=conn)
        df['wgr'] = df['warengruppe'].str.split(
            '|', expand=True)[0].str.strip()
        df['wgr_bez'] = df['warengruppe'].str.split('|', expand=True)[
            1].str.strip()
        df['ma_id'] = df['ma'].str.split('|', expand=True)[0]

        df['wgr'] = df['wgr'].where(~(df['warengruppe'].str.contains('fehlt', case=False) & df['art_bez'].str.contains(
            'einzahlung', case=False) & df['preis_gesamt'] != 0), "9001:1")
        df['wgr'] = df['wgr'].where(~(df['warengruppe'].str.contains('fehlt', case=False) & df['art_bez'].str.contains(
            'auszahlung', case=False) & df['preis_gesamt'] != 0), "9001:2")
        df['wgr'] = df['wgr'].where(
            ~(df['wgr'].astype(str).str.contains('fehlt', case=False)), "0:0")

        df['hash_bon'] = (df['kasse_nr'].astype(str) + ":" + df['bon_nr'].astype(str)
                          ).apply(lambda data: md5(data.encode('utf-8')).hexdigest())

        df = df.drop(
            columns=[
                'bon_beginn', 'bon_abschluss', 'ma', 'bon_summe',
                'typ', 'kdnr', 'bon_typ', 'warengruppe', 'storno_ref',
                'tse_info', 'infotext'].copy()
        )

        df['hash_fuehrend'] = None
        old_pos = None
        for i in range(len(df)):
            if not old_pos:
                old_pos = df.loc[i, "hash"]

            if not df.loc[i, "art_nr"] == '-':
                df.loc[i, 'hash_fuehrend'] = df.loc[i, "hash"]
                old_pos = df.loc[i, "hash"]
            else:
                df.loc[i, 'hash_fuehrend'] = old_pos
        conn.execute(self.tab_bon_pos_temp.delete())
        df.to_sql(self.tab_bon_pos_temp.name, conn,
                  index=False, if_exists='append')

    def _belade_bon_pos(self, conn: Connection) -> None:
        '''Aus der Bonpositionen-Zwischentabelle wird die Bonpositionen-Zieltabelle befuellt'''

        sql = '''
        INSERT INTO kassenbons_pos_t
        SELECT
            bt.*
            
        FROM temp_kassenbons_pos_t AS bt

        LEFT JOIN kassenbons_pos_t AS b
            ON	bt.hash = b.hash

        WHERE b.hash IS NULL
        '''
        conn.execute(text(sql))

    def _belade_kalender(self, conn: Connection) -> None:
        '''
        Laedt die vergebenen Datuemer in den Bons, ermittelt dazu Jahr, Monat, Tag, Wochentag und Kalenderwoche und schreibt diese in die Kalender-Zwischentabelle
        '''
        sql = 'SELECT DISTINCT kb.bon_datum FROM kassenbons_t AS kb ORDER BY 1'
        df_tage = pd.read_sql(text(sql), conn)

        df_tage['bon_datum'] = pd.to_datetime(df_tage['bon_datum'])
        df_tage['datum'] = df_tage['bon_datum'].dt.strftime('%Y-%m-%d').astype(str)
        df_tage['jahr'] = df_tage['bon_datum'].dt.strftime('%Y').astype(int)
        df_tage['monat'] = df_tage['bon_datum'].dt.strftime('%m').astype(int)
        df_tage['tag'] = df_tage['bon_datum'].dt.strftime('%d').astype(int)
        df_tage['wtag'] = df_tage['bon_datum'].dt.strftime('%u').astype(int)
        df_tage['kw'] = df_tage['bon_datum'].dt.strftime('%V').astype(int)
        df_tage = df_tage.drop(columns=['bon_datum'])

        conn.execute(text('DELETE FROM temp_kalender_t'))
        df_tage.to_sql('temp_kalender_t', conn, if_exists='append', index=False)

        sql_upd = """
        INSERT INTO kalender_t (datum, jahr, monat, tag, wtag, kw)
        SELECT
            tk.datum,
            tk.jahr,
            tk.monat,
            tk.tag,
            tk.wtag,
            tk.kw

        FROM temp_kalender_t as tk

        LEFT JOIN kalender_t as k
            ON	tk.datum = k.datum
            
        WHERE k.datum IS NULL
        """
        conn.execute(text(sql_upd))

class KassenjournalStatus():
    '''Holt Informationen zu den gespeicherten Kassenjournaldaten'''

    def __init__(self, db_manager: DbManager) -> None:
        super().__init__()
        self.db_manager = db_manager

    @property
    def monate(self) -> List[str]:
        '''Liefert eine Liste der importierten Monate'''

        SQL = """
        SELECT DISTINCT
            strftime('%Y-%m', bons.bon_datum) as monat
            
        FROM kassenbons_t AS bons

        ORDER BY 1
        """
        result = None
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL))
        conn.close()
        return [mon[0] for mon in result] or []

    @property
    def letzte_aenderung(self) -> datetime:
        '''
        Ermittelt die letzte Änderung in der Datenbank.
        Wurde also ein Import ausgeführt, der keine Veränderung bewirkte, 
        weil z.B. ein Protokoll mehrfach importiert wurde, wird auch keine Änderung dokumentiert
        '''
        SQL = """
        SELECT
            MAX(datetime(bons.eintrag_ts)) as zeitpunkt
            
        FROM kassenbons_t AS bons
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S')
            else:
                return None
        conn.close()
