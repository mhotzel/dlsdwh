from datetime import datetime, date
from hashlib import md5

import pandas as pd
from sqlalchemy import Connection, Table, text

from model.db_manager import DbManager


class WarengruppenImporter():
    '''Uebernimmt den Import der Warengruppen in die Datenbank'''

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
        self.tab_temp: Table = self.db_manager.meta_data.tables['temp_warengruppen_t']

        conn = self.db_manager.get_engine().connect()
        with conn:
            conn.execute(self.tab_temp.delete())

            self.df.to_sql(self.tab_temp.name, conn,
                           if_exists='append', index=False)
            conn.commit()
        conn.close()

    def load_file(self) -> None:
        '''
        Startet den Import der Warengruppen in die Zwischentabelle. Nach der Beladung der Zwischentabelle
        muss dann die Uebertragung in die Zieltabelle(n) mittels ::update_table gestartet werden.
        '''

        df_wgr = pd.read_csv(
            self.import_file, sep=';', decimal=',', encoding='cp1252',
            usecols=[
                'WGR_NR', 'UWGR-NR', 'Bezeichnung', 'MwSt.-KZ', 'Rabatt', 'FSKKZ'
            ]
        ).rename(columns={
            'WGR_NR': 'wgr',
            'UWGR-NR': 'uwgr',
            'Bezeichnung': 'wgr_bez',
            'MwSt.-KZ': 'mwst_kz',
            'Rabatt': 'rabatt_kz',
            'FSKKZ': 'fsk_kz'}
        )

        df_wgr['wgr'] = df_wgr.wgr.astype(str).str.cat(
            df_wgr.uwgr.astype(str), sep=':')
        df_wgr['eintrag_ts'] = pd.to_datetime(self.ts)
        df_wgr['export_datum'] = pd.to_datetime(self.export_date)
        df_wgr = df_wgr.drop(columns=['uwgr'])

        def mwst(val) -> float:
            if val == 4:
                return 19.0
            elif val == 9:
                return 7.0
            else:
                return 0.0

        df_wgr['mwst_satz'] = df_wgr['mwst_kz'].apply(mwst)
        df_wgr['hash_diff'] = (df_wgr['wgr_bez'] + ':' + df_wgr['mwst_kz'].astype(str) + ":" + df_wgr['mwst_satz'].astype(
            str) + ':' + df_wgr['rabatt_kz'] + ':' + df_wgr['fsk_kz'].astype(str)).apply(lambda s: md5(s.encode('utf-8')).hexdigest())
        df_wgr['hash'] = df_wgr['wgr'].apply(
            lambda s: md5(s.encode('utf-8')).hexdigest())
        df_wgr['quelle'] = 'scs_export_warengruppen'
        df_wgr['mwst_kz'] = df_wgr['mwst_kz'].astype(str)
        df_wgr['fsk_kz'] = df_wgr['fsk_kz'].astype(str)

        self.df = df_wgr

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
        INSERT INTO hub_warengruppen_t (hash, eintrag_ats, gueltig_adtm, zuletzt_gesehen, quelle, wgr)
        SELECT 
            t.hash,
            t.eintrag_ts AS eintrag_ats,
            t.export_datum AS gueltig_adtm,
            t.export_datum AS zuletzt_gesehen,
            t.quelle,
            t.wgr
            
        FROM temp_warengruppen_t as t

        LEFT JOIN hub_warengruppen_t AS h
            ON 	t.hash = h.hash

        WHERE h.hash IS NULL
        '''

        conn.execute(text(sql))

    def _loesche_ungueltige_sat(self, conn: Connection) -> None:
        '''
        Setzt Eintraege in 'sat_warengruppen_t' ungueltig, fuer die aktualisierte Eintraege
        vorhanden sind. Bestehende Eintraege, die nicht in der Eingabedatei vorkommen, bleiben
        unberuehrt.
        '''
        sql = '''
        UPDATE sat_warengruppen_t
        SET 
            eintrag_ets = :eintrag_ets,
            gueltig_edtm = :gueltig_edtm,
            gueltig = 0

        WHERE hash IN (
            SELECT 
                ws.hash
                
            FROM sat_warengruppen_t as ws

            LEFT JOIN temp_warengruppen_t AS wt
                ON	wt.hash = ws.hash

            WHERE	ws.gueltig = 1
            AND 	wt.hash_diff <> ws.hash_diff
        )
        '''
        conn.execute(text(sql), {'eintrag_ets': self.ts, 'gueltig_edtm': self.export_date})

    def _fuege_neue_sat_ein(self, conn: Connection) -> None:
        '''
        Fuegt neue Eintraege aus in 'sat_warengruppen_t' ein bzw. Eintraege, fuer die aktuellere
        Daten vorhanden sind.
        '''
        sql = '''
        INSERT INTO sat_warengruppen_t (hash, hash_diff, eintrag_ats, eintrag_ets, gueltig_adtm, gueltig_edtm, gueltig, quelle, wgr_bez, mwst_kz, mwst_satz, rabatt_kz, fsk_kz)
        SELECT 
            wt.hash,
            wt.hash_diff,
            wt.eintrag_ts AS eintrag_ats,
            datetime('2099-12-31 23:59:59.999999') AS eintrag_ets,
            :gueltig_adtm AS gueltig_adtm,
            date('2099-12-31') AS gueltig_edtm,
            1 as gueltig,
            wt.quelle,
            wt.wgr_bez,
            wt.mwst_kz,
            wt.mwst_satz,
            wt.rabatt_kz,
            wt.fsk_kz
            
        FROM temp_warengruppen_t as wt

        LEFT JOIN sat_warengruppen_t AS ws
            ON	wt.hash = ws.hash
            AND ws.gueltig = 1

        WHERE ws.hash IS NULL
        '''
        conn.execute(text(sql), {'gueltig_adtm': self.export_date})

    def _update_zuletzt_gesehen(self, conn: Connection) -> None:
        '''Setzt das 'zuletzt_gesehen'-Datum im HUB'''
        sql = '''
        UPDATE hub_warengruppen_t
        SET zuletzt_gesehen = bas.export_datum
        FROM (
            SELECT
                wt.export_datum,
                wt.hash

            FROM temp_warengruppen_t AS wt
            JOIN hub_warengruppen_t AS wh
                ON wh.hash = wt.hash
        ) AS bas
        WHERE bas.hash = hub_warengruppen_t.hash
        '''
        conn.execute(text(sql))


class WarengruppenStatus():
    '''Holt Informationen zu den gespeicherten Warengruppendaten'''

    def __init__(self, db_manager: DbManager) -> None:
        super().__init__()
        self.db_manager = db_manager

    @property
    def letzte_datei(self) -> datetime:
        '''
        Ermittelt das Datum der Datei mit dem jüngsten Import in der Datenbank.
        Dazu wird das neueste 'zuletzt_gesehen'-Datum ermittelt
        '''
        SQL = """
        SELECT MAX(hw.zuletzt_gesehen) AS zeitpunkt FROM hub_warengruppen_t AS hw
        """
        conn = self.db_manager.get_engine().connect()
        with conn:
            result = conn.execute(text(SQL)).fetchone()
            if result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d %H:%M:%S.%f')
            else:
                return None
        conn.close()
