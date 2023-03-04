from pathlib import Path

from sqlalchemy import (TIMESTAMP, URL, BigInteger, Boolean, Column, Date,
                        DateTime, Engine, Integer, MetaData, Numeric, String,
                        Table, create_engine)


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
            self.tables['tab_kassenjournal'] = Table(
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
            self.tables['tab_kassenjournal_temp'] = Table(
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
            self.tables['tab_kassenbons'] = Table(
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
            self.tables['tab_kassenbons_temp'] = Table(
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
            self.tables['tab_bon_pos_temp'] = Table(
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
            self.tables['tab_bon_pos'] = Table(
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
            self.tables['tab_hub_warengruppen'] = Table(
                'hub_warengruppen_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('wgr', String(40))
            )
            self.tables['tab_sat_warengruppen'] = Table(
                'sat_warengruppen_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('gueltig_bis', TIMESTAMP()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('wgr_bez', String(255)),
                Column('mwst_kz', String(2)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('rabatt_kz', String(1)),
                Column('fsk_kz', String(12))
            )
            self.tables['tab_warengruppen_temp'] = Table(
                'temp_warengruppen_t', self.meta_data,
                Column('quelle', String(255)),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255)),
                Column('mwst_kz', String(2)),
                Column('mwst_satz', Numeric(5, 2)),
                Column('rabatt_kz', String(1)),
                Column('fsk_kz', String(12))
            )
            self.tables['tab_kunden_temp'] = Table(
                'temp_kunden_t', self.meta_data,
                Column('quelle', String(255)),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kdnr', String(255)),
                Column('kd_name', String(255)),
                Column('rabatt_satz', Numeric(5, 2))
            )
            self.tables['tab_hub_kunden'] = Table(
                'hub_kunden_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('kdnr', String(255))
            )
            self.tables['tab_sat_kunden'] = Table(
                'sat_kunden_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('gueltig_bis', TIMESTAMP()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('kd_name', String(255)),
                Column('rabatt_satz', Numeric(5, 2))
            )
            self.tables['tab_artikel_temp'] = Table(
                'temp_artikel_t', self.meta_data,
                Column('quelle', String(255)),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
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
            self.tables['tab_hub_artikel'] = Table(
                'hub_artikel_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('art_nr', String(255))
            )
            self.tables['tab_sat_artikel'] = Table(
                'sat_artikel_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('gueltig_bis', TIMESTAMP()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
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
            self.tables['tab_pfand_temp'] = Table(
                'temp_pfand_t', self.meta_data,
                Column('quelle', String(255)),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('art_nr', String(40)),
                Column('pfand_bez', String(255)),
                Column('pfand_brutto', Numeric(18, 2)),
                Column('hinweispflicht', String(40)),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255))
            )
            self.tables['tab_hub_pfand'] = Table(
                'hub_pfand_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('art_nr', String(255))
            )
            self.tables['tab_sat_pfand'] = Table(
                'sat_pfand_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('gueltig_bis', TIMESTAMP()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('pfand_bez', String(255)),
                Column('pfand_brutto', Numeric(18, 2)),
                Column('hinweispflicht', String(40)),
                Column('wgr', String(40)),
                Column('wgr_bez', String(255))
            )
            self.tables['tab_lieferanten_temp'] = Table(
                'temp_lieferanten_t', self.meta_data,
                Column('quelle', String(255)),
                Column('hash', String(40), primary_key=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('lief_nr', String(255)),
                Column('lief_kdnr', String(255)),
                Column('lief_name', String(255)),
                Column('ek_art_uebernahme', String(12)),
                Column('ist_hauptlief', String(12)),
                Column('art_import_logik', String(12))
            )
            self.tables['tab_hub_lieferanten'] = Table(
                'hub_lieferanten_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('zuletzt_gesehen', TIMESTAMP()),
                Column('quelle', String(255)),
                Column('lief_nr', String(255))
            )
            self.tables['tab_sat_lieferanten'] = Table(
                'sat_lieferanten_t', self.meta_data,
                Column('hash', String(40), index=True),
                Column('hash_diff', String(40)),
                Column('eintrag_ts', TIMESTAMP()),
                Column('gueltig_bis', TIMESTAMP()),
                Column('gueltig', Boolean(create_constraint=True)),
                Column('quelle', String(255)),
                Column('lief_kdnr', String(255)),
                Column('lief_name', String(255)),
                Column('ek_art_uebernahme', String(12)),
                Column('ist_hauptlief', String(12)),
                Column('art_import_logik', String(12))
            )
        return self.meta_data
