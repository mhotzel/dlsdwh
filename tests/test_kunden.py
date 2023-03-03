from pathlib import Path
import sys

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))

from model.db_manager import DbManager
from model.kunden import KundenImporter

def test_kdn_1(db_man: DbManager):
    '''Einfuegen neuer Kundendaten'''

    import_file = r"C:\Users\matth\Desktop\DLS\scs-daten_20230301\Kunden_1.txt"

    kdn_importer = KundenImporter(db_man, import_file)

    kdn_importer.load_file()
    print(kdn_importer.df)
    kdn_importer.write_data()
    kdn_importer.post_process()

def test_kdn_2(db_man: DbManager):
    '''
    Hinzufuegen neuer Eintraege, aber einer ist schon vorhanden
    und darf nicht ueberschrieben werden.
    Der Eintrag, der in der DB vorhanden ist, aber nicht mehr in
    der Eingabedatei, bleibt unveraendert.
    '''

    db_file = r"C:\Users\matth\Desktop\dlsdwh.db"
    import_file = r"C:\Users\matth\Desktop\DLS\scs-daten_20230301\Kunden_2.txt"

    db_man = DbManager(db_file)
    kdn_importer = KundenImporter(db_man, import_file)

    kdn_importer.load_file()
    print(kdn_importer.df)
    kdn_importer.write_data()
    kdn_importer.post_process()

def test_kdn_3(db_man: DbManager):
    '''
    Bestehende Eintraege werden veraendert, es muss ein bestehender vorhandener 
    Eintrag im SAT ungueltig gesetzt und der neue veraenderte Wert als gueltig 
    eingesetzt werden
    '''

    db_file = r"C:\Users\matth\Desktop\dlsdwh.db"
    import_file = r"C:\Users\matth\Desktop\DLS\scs-daten_20230301\Kunden_3.txt"

    db_man = DbManager(db_file)
    kdn_importer = KundenImporter(db_man, import_file)

    kdn_importer.load_file()
    print(kdn_importer.df)
    kdn_importer.write_data()
    kdn_importer.post_process()

if __name__ == '__main__':
    db_file = r"C:\Users\matth\Desktop\dlsdwh.db"
    Path(db_file).unlink(True)
    
    db_man = DbManager(db_file)
    db_man.get_metadata().create_all(db_man.get_engine())

    test_kdn_1(db_man)
    test_kdn_2(db_man)
    #test_kdn_3(db_man)
    