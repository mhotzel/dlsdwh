from pathlib import Path
from sqlalchemy import TIMESTAMP, BigInteger, Column, DateTime, Engine, Integer, MetaData, Numeric, String, Table, create_engine, URL, Date


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

        return create_engine(url)

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
                Column('kdnr', Integer(), index=True),
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
                'kassenjournal_temp_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer(), index=True),
                Column('bon_nr', BigInteger(), index=True),
                Column('pos', Integer()),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('ma', String(40)),
                Column('kdnr', Integer(), index=True),
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
                Column('kdnr', Integer(), index=True),
                Column('bon_summe', Numeric(18, 2))
            )
            self.tables['tab_kassenbons_temp'] = Table(
                'kassenbons_temp_t', self.meta_data,
                Column('hash', String(40), primary_key=True),
                Column('eintrag_ts', TIMESTAMP()),
                Column('kasse_nr', Integer()),
                Column('bon_nr', BigInteger(), index=True),
                Column('bon_typ', String(12), index=True),
                Column('bon_beginn', DateTime()),
                Column('bon_abschluss', DateTime()),
                Column('bon_datum', Date(), index=True),
                Column('kdnr', Integer(), index=True),
                Column('bon_summe', Numeric(18, 2))
            )
        return self.meta_data
