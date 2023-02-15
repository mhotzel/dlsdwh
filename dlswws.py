from configparser import ConfigParser
from tkinter.messagebox import showerror

from settings import (CONFIG_FILE, check_configfile, create_tables,
                      get_dbconfig, set_highdpi, set_lang)
from view.main_window import MainWindow


def get_config() -> ConfigParser:
    '''liest die Konfiguration und gibt diese zurück'''
    cfg_parser = ConfigParser()
    cfg_parser.read(CONFIG_FILE)
    return cfg_parser


def main() -> None:

    set_lang()
    set_highdpi()
    check_configfile()
    dbfile = get_dbconfig()
    if not dbfile:
        showerror(title='Konfigurationsfehler',
                  message='Es wurde keine Datenbank ausgewählt. Deshalb wird die Anwendung nun beendet')
        return
    create_tables()

    main_win = MainWindow(get_config())
    main_win.mainloop()


if __name__ == '__main__':
    main()
