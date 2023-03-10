from datetime import date, datetime
import textwrap
from ttkbootstrap import (Frame, Label, DateEntry, Separator, Button)
from ttkbootstrap.dialogs import QueryDialog


class SelectDateWidget(QueryDialog):
    '''Datumsauswahl'''

    def __init__(self):
        self.export_date = None

        super().__init__(prompt="Exportdatum der einzulesenden Datei eingeben",
                         title="Dateiexportdatum auswählen")

    def create_buttonbox(self, master):
        """Overrides the parent method; adds the message buttonbox"""
        frame = Frame(master, padding=(5, 10))

        submit = Button(
            master=frame,
            bootstyle="primary",
            text='OK',
            command=self.on_submit,
        )
        submit.pack(padx=5, side='right')
        submit.lower()  # set focus traversal left-to-right

        cancel = Button(
            master=frame,
            bootstyle="secondary",
            text="Abbrechen",
            command=self.on_cancel,
        )
        cancel.pack(padx=5, side='right')
        cancel.lower()  # set focus traversal left-to-right

        Separator(self._toplevel).pack(fill='x')
        frame.pack(side='bottom', fill='x', anchor='s')

    def create_body(self, master):
        """Overrides the parent method; adds the message and input
        section."""
        frame = Frame(master, padding=self._padding)
        if self._prompt:
            for p in self._prompt.split("\n"):
                prompt = "\n".join(textwrap.wrap(p, width=self._width))
                prompt_label = Label(frame, text=prompt)
                prompt_label.pack(pady=(0, 5), fill='x', anchor='n')

        entry = DateEntry(master=frame, bootstyle='secondary',
                          dateformat='%d.%m.%Y', firstweekday=0, startdate='')
        entry.pack(pady=(0, 5), fill='x')
        entry.bind("<Return>", self.on_submit)
        entry.bind("<KP_Enter>", self.on_submit)
        entry.bind("<Escape>", self.on_cancel)
        frame.pack(fill='x', expand=True)
        self._initial_focus = entry

    def on_submit(self, *_):
        """Save result, destroy the toplevel, and apply any post-hoc
        data manipulations."""
        self._result: str = self._initial_focus.entry.get()
        valid_result = self.validate()
        if not valid_result:
            return  # keep toplevel open for valid response
        self.export_date: date = datetime.strptime(self._result, '%d.%m.%Y').date()
        self._toplevel.destroy()
        self.apply()
