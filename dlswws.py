from tkinter.messagebox import showerror, showinfo

from settings import (check_configfile, create_tables,
                      get_dbconfig, set_highdpi, set_lang, get_config, select_database)
from view.main_window import MainWindow


def main() -> None:

    set_lang()
    set_highdpi()
    check_configfile()
    new_database_set = False
    dbfile = get_dbconfig()
    if not dbfile:
        new_database_set = True
        dbfile = select_database()
    
    if not dbfile:
        showerror(title='Konfigurationsfehler',
                  message='Es wurde keine Datenbank ausgewählt. Deshalb wird die Anwendung nun beendet')
        return
    create_tables()

    if new_database_set:
        showinfo('Neustart erforderlich', 'Das Programm wird nun beenden. Bitte die Anwendung nun neu starten, um die Änderung wirksam werden zu lassen.')
        return

    main_win = MainWindow(get_config())
    main_win.mainloop()


if __name__ == '__main__':
    main()
