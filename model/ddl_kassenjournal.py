'''Definiert die SQL-Statements zur Erzeugung der Kassenjournal-Tabellen'''

SQL_DDL = ["""
CREATE TABLE IF NOT EXISTS kassenjournal_temp_t
(
    eintrag_ts DATETIME,
    kasse_nr INT,
    bon_nr BIGINT,
    pos BIGINT,
    bon_beginn DATETIME,
    bon_abschluss DATETIME,
    verkauefer TEXT,
    kdnr INT,
    bon_summe FLOAT,
    typ TEXT,
    art_nr TEXT,
    art_bez TEXT,
    warengruppe TEXT,
    mwst_satz FLOAT,
    mengenfaktor INT,
    menge INT,
    preis_einzel FLOAT,
    preis_gesamt FLOAT,
    infotext TEXT,
    storno_ref TEXT,
    tse_info TEXT
)
""", """
CREATE TABLE IF NOT EXISTS kassenjournal_t
(
    eintrag_ts DATETIME,
    kasse_nr INT,
    bon_nr BIGINT,
    pos BIGINT,
    bon_beginn DATETIME,
    bon_abschluss DATETIME,
    verkauefer TEXT,
    kdnr INT,
    bon_summe FLOAT,
    typ TEXT,
    art_nr TEXT,
    art_bez TEXT,
    warengruppe TEXT,
    mwst_satz FLOAT,
    mengenfaktor INT,
    menge INT,
    preis_einzel FLOAT,
    preis_gesamt FLOAT,
    infotext TEXT,
    storno_ref TEXT,
    tse_info TEXT
)
""", """
CREATE INDEX IF NOT EXISTS idx_kjt_bon_nr ON kassenjournal_temp_t(bon_nr)
""", """
CREATE INDEX IF NOT EXISTS idx_kj_bon_nr ON kassenjournal_t(bon_nr)
""", """
CREATE INDEX IF NOT EXISTS idx_kj_bon_abschl ON kassenjournal_t(bon_abschluss)
""", """
CREATE INDEX IF NOT EXISTS idx_kj_bon_beginn ON kassenjournal_t(bon_beginn)
""","""
CREATE INDEX IF NOT EXISTS idx_kj_eintragts ON kassenjournal_t(eintrag_ts)
"""
       ]
