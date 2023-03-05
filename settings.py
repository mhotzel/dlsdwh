from configparser import ConfigParser
import locale
import os
from pathlib import Path
from tkinter.filedialog import asksaveasfilename
from typing import Mapping, Tuple
from model.db_manager import DbManager

BASEDIR = str(Path(__file__).parent)
IMGDIR = str(Path(BASEDIR) / 'res' / 'img')
CONFIG_FILE = str(Path('~/.dlswws.ini').expanduser().absolute())
LOG_FILE = str(Path('~/dlswws.log').expanduser().absolute())


def get_config() -> Mapping[str, str]:
    '''liest die Konfiguration und gibt diese zurück'''
    cfg_parser = ConfigParser()
    cfg_parser.read(CONFIG_FILE)

    cfg = dict()
    for section in cfg_parser.sections():
        for key, val in cfg_parser[section].items():
            cfg[key] = val

    return cfg


def set_lang():
    '''Setzt die Sprache'''
    if not "LANG" in os.environ:
        os.environ['LANG'] = 'de_DE.UTF-8'

    locale.setlocale(locale.LC_ALL, os.environ['LANG'])


def set_highdpi():
    '''Setzt die hohe Auflösung unter Windows'''
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except ImportError:
        # only available on windows
        pass


def check_configfile() -> None:
    '''Legt bei Bedarf die Konfigurationsdatei an'''

    cfg_parser = ConfigParser()
    cfg_parser.read(CONFIG_FILE)

    if 'datenbank' in cfg_parser.sections():
        return

    cfg_parser.add_section('datenbank')
    with open(CONFIG_FILE, mode='w') as cfgfile:
        cfg_parser.write(cfgfile)


def get_dbconfig() -> str:
    '''Liefert die Datenbankkonfiguration - aktuell als Dateiname der SQLITE-Datenbank'''

    cfg_parser = ConfigParser()
    cfg_parser.read(CONFIG_FILE)

    if 'dbfile' in cfg_parser['datenbank']:
        return cfg_parser['datenbank']['dbfile']

    return None


def select_database() -> str:
    '''Ruft den Dialog zur Auswahl einer SQLITE-Datenbank auf'''

    cfg_parser = ConfigParser()
    cfg_parser.read(CONFIG_FILE)

    filename = asksaveasfilename(
        title='bestehende Datenbank auswählen oder einen neuen Dateinamen eingeben',
        defaultextension='Sqlite Datenbank (*.db)',
        filetypes=[('Sqlite Datenbank', '*.db'),
                   ('Sqlite Datenbank', '*.sqlite')],
        confirmoverwrite=False)

    if filename:
        filename = str(Path(filename).resolve())
        cfg_parser['datenbank']['dbfile'] = str(Path(filename).resolve())

    with open(CONFIG_FILE, mode='w') as cfgfile:
        cfg_parser.write(cfgfile)

    return filename


def create_tables() -> None:
    db_man = DbManager(get_dbconfig())
    md = db_man.get_metadata()
    md.create_all(db_man.get_engine())
