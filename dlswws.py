from tkinter.messagebox import showerror

from model.controller import Controller
from settings import CONFIG_FILE, IMGDIR
from settings import check_configfile, get_dbconfig, set_highdpi, set_lang
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

    controller = Controller()
    main_win = MainWindow(controller)
    main_win.set_status_message(f"Datenbankdatei: '{dbfile}'")
    main_win.mainloop()


if __name__ == '__main__':
    main()
