import inspect

import datajoint as dj


class Experiment(dj.Imported):
    original_definition = """  # information about experiments
    -> Subject
    experiment_id  :int16  # experiment number for this subject
    ---
    experiment_date  :date   # date when experiment was started
    -> [nullable] User
    data_path=""     :varchar(255)  # file path to recorded data
    notes=""         :varchar(2048) # e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :datetime   # automatic timestamp
    """

    definition1 = """  # Experiment
    -> Subject
    experiment_id  :int16  # experiment number for this subject
    ---
    data_path     : int32  # some number
    extra=null : <blob>  # just testing
    -> [nullable] User
    subject_notes=null         :varchar(2048) # {notes} e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :datetime   # automatic timestamp
    """


class Parent(dj.Manual):
    definition = """
    parent_id: int32
    """

    class Child(dj.Part):
        definition = """
        -> Parent
        """
        definition_new = """
        -> master
        ---
        child_id=null: int32
        """

    class Grandchild(dj.Part):
        definition = """
        -> master.Child
        """
        definition_new = """
        -> master.Child
        ---
        grandchild_id=null: int32
        """


LOCALS_ALTER = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_ALTER)
