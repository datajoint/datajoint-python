from .relational_operand import RelationalOperand
from .utils import from_camel_case
from . import DataJointError


class View(RelationalOperand):

    def definition(self):
        """
        :return: a query to be defined as view.
        """
        raise DataJointError('Subclasses of BaseRelation must implement the property "definition"')

    def declare(self):
        """
        Declare the view in the database.  Is called by the schema decorator
        """
        if not isinstance(self.definition, RelationalOperand):
            raise DataJointError('The definition of a view must be a relational expression')
        self.connection.query("CREATE OR REPLACE VIEW {view} AS {select}".format(
                view=self.from_clause,
                select=self.definition.make_sql()))

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return '`{db}`.`{view}`'.format(
            db=self.database,
            view=from_camel_case(self.__class__.__name__))

    @property
    def select_fields(self):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return '*'

    @property
    def heading(self):
        return self.definition.heading
