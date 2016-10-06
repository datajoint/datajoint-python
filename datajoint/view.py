from .relational_operand import RelationalOperand
from .utils import from_camel_case, ClassProperty
from . import DataJointError


class View(RelationalOperand):

    _connection = None
    _database = None

    @ClassProperty
    def definition(self):
        """
        :return: a query to be defined as view.
        """
        raise NotImplementedError('Subclasses of BaseRelation must implement the property "definition"')

    def declare(self, connection, database):
        """
        Declare the view in the database.  Is called by the schema decorator
        """
        if not isinstance(self.definition, RelationalOperand):
            raise DataJointError('The d')
        self._database = database
        self._connection = connection
        self.connection.query(
            """
            CREATE OR REPLACE VIEW {view} AS {select}
            """.format(
                view=self.from_clause,
                select=self.definition.make_sql()))

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return '`{db}`.`{view}`'.format(
            db=self._database,
            view=from_camel_case(self.__class__.__name__))

    @property
    def select_fields(self):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return '*'
