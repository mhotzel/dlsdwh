from pathlib import Path
import pandas as pd

from sqlalchemy import (TIMESTAMP, URL, BigInteger, Boolean, Column, Date,
                        DateTime, Engine, Integer, MetaData, Numeric, String,
                        Table, create_engine)

def concat(df: pd.DataFrame):
    '''Bereitet ein DataFrame so auf, dass es einfacher gehasht werden kann'''
    res = None
    for i, col in enumerate(df.columns):
        if i == 0:
            res = df[col].astype(str).str.strip()
        else:
            res += ':' + df[col].astype(str).str.strip()
    return res

class DbManager():
    '''Managed die Datenbankverbindung'''

    def __init__(self, dbfile: str) -> None:
        self.dbfile = dbfile
        self.meta_data = None
        self.tables = dict()
        self.get_metadata()

    def get_engine(self) -> Engine:
        '''
        Erzeugt eine Datenbankverbindung.
        '''
        url = URL.create(
            drivername='sqlite',
            database=self.dbfile
        )

        engine = create_engine(url, echo=False)
        return engine

    def get_metadata(self) -> MetaData:
        'liefert die Metadaten zur Datenbank. Lazy-Init.'
        if not self.meta_data:
            self.meta_data = MetaData()
            Table(
                'kassenjournal_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer(), index=True),
                Column('bon_nr', BigInteger(), index=True),
                Column('pos', Integer()),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('ma', String(40)),
                Column('kdnr', String(255), index=True),
                Column('bon_summe', Numeric(18, 2)),
                Column('typ', String(40)),
                Column('bon_typ', String(12)),
                Column('pos_typ', String(12)),
                Column('art_nr', String(40), index=True),
                Column('art_bez', String(255)),
                Column('warengruppe', String(255)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('mengenfaktor', Integer()),
                Column('menge', Numeric(18, 3)),
                Column('preis_einzel', Numeric(18, 2)),
                Column('preis_gesamt', Numeric(18, 2)),
                Column('infotext', String(255)),
                Column('storno_ref', BigInteger()),
                Column('tse_info', String(255))
            )
            Table(
                'temp_kassenjournal_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer(), index=True),
                Column('bon_nr', BigInteger(), index=True),
                Column('pos', Integer()),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('ma', String(40)),
                Column('kdnr', String(255), index=True),
                Column('bon_summe', Numeric(18, 2)),
                Column('typ', String(40)),
                Column('bon_typ', String(12)),
                Column('pos_typ', String(12)),
                Column('art_nr', String(40), index=True),
                Column('art_bez', String(255)),
                Column('warengruppe', String(255)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('mengenfaktor', Integer()),
                Column('menge', Numeric(18, 3)),
                Column('preis_einzel', Numeric(18, 2)),
                Column('preis_gesamt', Numeric(18, 2)),
                Column('infotext', String(255)),
                Column('storno_ref', BigInteger()),
                Column('tse_info', String(255))
            )
            Table(
                'kassenbons_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer()),
                Column('bon_nr', BigInteger(), index=True),
                Column('bon_typ', String(12), index=True),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('bon_datum', Date(), index=True),
                Column('kdnr', String(255), index=True),
                Column('bon_summe', Numeric(18, 2)),
                Column('tse_info', String(255)),
                Column('storno_ref', Integer())
            )
            Table(
                'temp_kassenbons_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer()),
                Column('bon_nr', BigInteger(), index=True),
                Column('bon_typ', String(12), index=True),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('bon_datum', Date(), index=True),
                Column('kdnr', String(255), index=True),
                Column('bon_summe', Numeric(18, 2)),
                Column('tse_info', String(255)),
                Column('storno_ref', Integer())
            )
            Table(
                'temp_kassenbons_pos_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('hash_bon', String(40)),
                Column('hash_fuehrend', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer()),
                Column('bon_nr', BigInteger()),
                Column('pos', Integer()),
                Column('pos_typ', String(12)),
                Column('art_nr', String(40)),
                Column('art_bez', String(255)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('mengenfaktor', Integer()),
                Column('menge', Numeric(18, 3)),
                Column('preis_einzel', Numeric(18, 2)),
                Column('preis_gesamt', Numeric(18, 2)),
                Column('wgr', Integer()),
                Column('wgr_bez', String(50)),
                Column('ma_id', Integer())
            )
            Table(
                'kassenbons_pos_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('hash_bon', String(40), index=True),
                Column('hash_fuehrend', String(40), index=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer()),
                Column('bon_nr', BigInteger(), index=True),
                Column('pos', Integer()),
                Column('pos_typ', String(12), index=True),
                Column('art_nr', String(40), index=True),
                Column('art_bez', String(255)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('mengenfaktor', Integer()),
                Column('menge', Numeric(18, 3)),
                Column('preis_einzel', Numeric(18, 2)),
                Column('preis_gesamt', Numeric(18, 2)),
                Column('wgr', String(40), index=True),
                Column('wgr_bez', String(50)),
                Column('ma_id', Integer())
            )
            Table(
                'kalender_t', self.meta_data,
                Column('datum', Date, primary_key=True),
                Column('jahr', Integer()),
                Column('monat', Integer()),
                Column('tag', Integer()),
                Column('wtag', Integer()),
                Column('kw', Integer())
            )
            Table(
                'temp_kalender_t', self.meta_data,
                Column('datum', Date(), primary_key=True),
                Column('jahr', Integer()),
                Column('monat', Integer()),
                Column('tag', Integer()),
                Column('wtag', Integer()),
                Column('kw', Integer())
            )
            Table(
                'hub_warengruppen_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('wgr', String(40), index=True)
            )
            Table(
                'sat_warengruppen_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('wgr_bez', String(255)),
                Column('mwst_kz', String(2)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('rabatt_kz', String(1)),
                Column('fsk_kz', String(12))
            )
            Table(
                'temp_warengruppen_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255)),
                Column('mwst_kz', String(2)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('rabatt_kz', String(1)),
                Column('fsk_kz', String(12))
            )
            Table(
                'temp_kunden_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('kdnr', String(255)),
                Column('kd_name', String(255)),
                Column('rabatt_satz', Numeric(5, 2))
            )
            Table(
                'hub_kunden_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('kdnr', String(255), index=True)
            )
            Table(
                'sat_kunden_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('kd_name', String(255)),
                Column('rabatt_satz', Numeric(5, 2))
            )
            Table(
                'temp_artikel_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('art_nr', String(40)),
                Column('idx', Integer()),
                Column('scs_pool_id', BigInteger()),
                Column('art_bez', String(255)),
                Column('mengenfaktor', Numeric(18, 3)),
                Column('vk_brutto', Numeric(18, 2)),
                Column('preiseinheit', Integer()),
                Column('kurzcode', String(40)),
                Column('bontext', String(255)),
                Column('mengeneinheit', String(50)),
                Column('mengentyp', String(50)),
                Column('gpfaktor', Numeric(6, 3)),
                Column('wgr', String(40)),
                Column('rabatt_kz', String(1)),
                Column('preisgebunden_kz', String(1)),
                Column('fsk_kz', String(1)),
                Column('notizen', String(255))
            )
            Table(
                'hub_artikel_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', Date()),
                Column('quelle', String(255)),
                Column('art_nr', String(255), index=True)
            )
            Table(
                'sat_artikel_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('idx', Integer()),
                Column('scs_pool_id', BigInteger()),
                Column('art_bez', String(255)),
                Column('mengenfaktor', Numeric(18, 3)),
                Column('vk_brutto', Numeric(18, 2)),
                Column('preiseinheit', Integer()),
                Column('kurzcode', String(40)),
                Column('bontext', String(255)),
                Column('mengeneinheit', String(50)),
                Column('mengentyp', String(50)),
                Column('gpfaktor', Numeric(6, 3)),
                Column('wgr', String(40)),
                Column('rabatt_kz', String(1)),
                Column('preisgebunden_kz', String(1)),
                Column('fsk_kz', String(1)),
                Column('notizen', String(255))
            )
            Table(
                'temp_pfand_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('art_nr', String(40)),
                Column('pfand_bez', String(255)),
                Column('pfand_brutto', Numeric(18, 2)),
                Column('hinweispflicht', String(40)),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255))
            )
            Table(
                'hub_pfand_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('art_nr', String(255), index=True)
            )
            Table(
                'sat_pfand_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('pfand_bez', String(255)),
                Column('pfand_brutto', Numeric(18, 2)),
                Column('hinweispflicht', String(40)),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255))
            )
            Table(
                'temp_lieferanten_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('lief_nr', String(40)),
                Column('lief_kdnr', String(255)),
                Column('lief_name', String(255)),
                Column('ek_art_uebernahme', String(12)),
                Column('ist_hauptlief', String(12)),
                Column('art_import_logik', String(12))
            )
            Table(
                'hub_lieferanten_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('lief_nr', String(40), index=True)
            )
            Table(
                'sat_lieferanten_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('lief_kdnr', String(255)),
                Column('lief_name', String(255)),
                Column('ek_art_uebernahme', String(12)),
                Column('ist_hauptlief', String(12)),
                Column('art_import_logik', String(12))
            )
            Table(
                'temp_mean_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('ean_m', String(40)),
                Column('ean_h', String(40))
            )
            Table(
                'hub_mean_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('ean_m', String(40), index=True),
            )
            Table(
                'sat_mean_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('ean_h', String(40))
            )
            Table(
                'temp_scs_liefart_t', self.meta_data,
                Column('quelle', String(255)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('export_datum', Date()),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('ean', String(40)),
                Column('lief_nr', String(40)),
                Column('lief_art_nr', String(40)),
                Column('ek_netto', Numeric(18, 3))
            )
            Table(
                'hub_scs_liefart_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ats', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('ean', String(40), index=True),
                Column('lief_nr', String(40), index=True),
            )
            Table(
                'sat_scs_liefart_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ats', TIMESTAMP()),
                Column('eintrag_ets', TIMESTAMP()),
                Column('gueltig_adtm', Date()),
                Column('gueltig_edtm', Date()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('lief_art_nr', String(40)),
                Column('ek_netto', Numeric(18, 3))
            )

        return self.meta_data
