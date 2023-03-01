from tkinter.messagebox import showerror

from settings import (check_configfile, create_tables,
                      get_dbconfig, set_highdpi, set_lang, get_config)
from view.main_window import MainWindow


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
