import datajoint as dj
from . import PREFIX, CONN_INFO



class TestDeclare:
    """
    Tests declaration, heading, dependencies, and drop
    """

    schema = dj.schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))

    @schema
    class Subjects(dj.Manual):
        definition = """
        #Basic subject
        subject_id                  : int      # unique subject id
        ---
        real_id                     :  varchar(40)    #  real-world name
        species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
        """

    @schema
    class Experiments(dj.Manual):
        definition = """
        # information of non-human subjects
        -> Subjects
        ---
        animal_dob      :date       # date of birth
        """

    @schema
    class Trials(dj.Manual):
        definition = """
        # info about trials
        -> Subjects
        trial_id: int
        ---
        outcome: int            # result of experiment
        notes="": varchar(4096) # other comments
        trial_ts=CURRENT_TIMESTAMP: timestamp     # automatic
        """

    @schema
    class Matrix(dj.Manual):
        definition = """
        # Some numpy array
        matrix_id: int       # unique matrix id
        ---
        data:    longblob   #  data
        comment: varchar(1000) # comment
        """