from threading import Thread
from time import sleep
from typing import Callable
import pandas as pd
from datetime import date, datetime
from model.db_manager import DbManager


class KassenjournalImporter():
    '''Uebernimmt den Import des Kassenjournals in die Datenbank'''

    def __init__(self, db_man: DbManager) -> None:
        self.db_man = db_man
        self._listeners = set()

    @property
    def kj_file(self) -> str:
        return self._kj_file

    @kj_file.setter
    def kj_file(self, kj_file: str) -> None:
        self._kj_file = kj_file

    def write_df(self) -> None:
        '''
        Schreibt die gelesenen Daten in die Datenbank.
        Wichtig. Zuerst muessen sie mit 'load_file' geladen werden.
        '''

        conn = self.db_man.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM kassenjournal_temp_t")
        self.df.to_sql('kassenjournal_temp_t', self.db_man.get_connection(),
                       if_exists='append', index=False)
        cur.close()

    def load_file(self) -> None:
        '''
        Startet den Import des Kassenjournals in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle mittels ::update_table gestartet werden.
        '''

        df = pd.read_csv(
            self.kj_file, sep=';', decimal=',', encoding='utf8',
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
            }, parse_dates=['Zeitpunkt', 'Beginn']
        ).rename(columns={
            'Kassen-Nr.': 'kasse_nr',
            'Bon-Nr.': 'bon_nr',
            'Beginn': 'bon_beginn',
            'Zeitpunkt': 'bon_abschluss',
            'Verkäufer': 'verkauefer',
            'Kunden-Nr.': 'kdnr',
            'Bon-Summe': 'bon_summe',
            'Typ': 'typ',
            'Artikelnummer': 'art_nr',
            'Bezeichnung': 'art_bez',
            'Warengruppe': 'warengruppe',
            'MwSt.-Satz': 'mwst_kz',
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
        df['eintrag_ts'] = datetime.now().isoformat()
        df['pos'] = df[['bon_nr']].groupby('bon_nr').cumcount()
        self.df = df

    def update_table(self) -> None:
        '''Nach der Beladung der Zwischentabelle wird mittels dieser Methode die Beladung der Zieltabelle gestartet.'''

        sql = '''
        INSERT INTO kassenjournal_t
        SELECT
            kjt.*
            
        FROM kassenjournal_temp_t AS kjt

        LEFT JOIN kassenjournal_t AS kj
            ON	kjt.kasse_nr = kj.kasse_nr
            AND kjt.bon_nr = kj.bon_nr

        WHERE kj.bon_nr IS NULL
        '''

        conn = self.db_man.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
