import datajoint as dj
import inspect


class Person(dj.Manual):
    definition = """
    person_id : int
    ----
    full_name : varchar(60)
    sex : enum('M','F')
    """

    def fill(self):
        """
        fill fake names from www.fakenamegenerator.com
        """
        self.insert(
            (
                (0, "May K. Hall", "F"),
                (1, "Jeffrey E. Gillen", "M"),
                (2, "Hanna R. Walters", "F"),
                (3, "Russel S. James", "M"),
                (4, "Robbin J. Fletcher", "F"),
                (5, "Wade J. Sullivan", "M"),
                (6, "Dorothy J. Chen", "F"),
                (7, "Michael L. Kowalewski", "M"),
                (8, "Kimberly J. Stringer", "F"),
                (9, "Mark G. Hair", "M"),
                (10, "Mary R. Thompson", "F"),
                (11, "Graham C. Gilpin", "M"),
                (12, "Nelda T. Ruggeri", "F"),
                (13, "Bryan M. Cummings", "M"),
                (14, "Sara C. Le", "F"),
                (15, "Myron S. Jaramillo", "M"),
            )
        )


class Parent(dj.Manual):
    definition = """
    -> Person
    parent_sex  : enum('M','F')
    ---
    -> Person.proj(parent='person_id')
    """

    def fill(self):
        def make_parent(pid, parent):
            return dict(
                person_id=pid,
                parent=parent,
                parent_sex=(Person & {"person_id": parent}).fetch1("sex"),
            )

        self.insert(
            make_parent(*r)
            for r in (
                (0, 2),
                (0, 3),
                (1, 4),
                (1, 5),
                (2, 4),
                (2, 5),
                (3, 4),
                (3, 7),
                (4, 7),
                (4, 8),
                (5, 9),
                (5, 10),
                (6, 9),
                (6, 10),
                (7, 11),
                (7, 12),
                (8, 11),
                (8, 14),
                (9, 11),
                (9, 12),
                (10, 13),
                (10, 14),
                (11, 14),
                (11, 15),
                (12, 14),
                (12, 15),
            )
        )


class Subject(dj.Manual):
    definition = """
    subject : int
    ---
    -> [unique, nullable] Person
    """


class Prep(dj.Manual):
    definition = """
    prep   : int
    """


class Slice(dj.Manual):
    definition = """
    -> Prep
    slice  : int
    """


class Cell(dj.Manual):
    definition = """
    -> Slice
    cell  : int
    """


class InputCell(dj.Manual):
    definition = """  # a synapse within the slice
    -> Cell
    -> Cell.proj(input="cell")
    """


class LocalSynapse(dj.Manual):
    definition = """  # a synapse within the slice
    -> Cell.proj(presynaptic='cell')
    -> Cell.proj(postsynaptic='cell')
    """


class GlobalSynapse(dj.Manual):
    # Mix old-style and new-style projected foreign keys
    definition = """
    # a synapse within the slice
    -> Cell.proj(pre_slice="slice", pre_cell="cell")
    -> Cell.proj(post_slice="slice", post_cell="cell")
    """


LOCALS_ADVANCED = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_ADVANCED)
