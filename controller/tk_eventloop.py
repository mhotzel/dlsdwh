from typing import Callable, Protocol

class TkEventLoop(Protocol):
    '''Interface zum TK-Eventsystem'''

    def after(self, ms: int, func: Callable = None) -> None:
        '''Die after-Methode aus dem tkinter Event Loop'''
    
    