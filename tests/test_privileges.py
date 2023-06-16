import datajoint as dj
import os
from packaging import version
import pytest
from . import PREFIX, connection_root, connection_test

# from .schemas.default import schema, Language
from .schemas.privileges import (
    schema,
    Parent,
    Child,
    NoAccess,
    NoAccessAgain,
)


@pytest.fixture
def connection_view(connection_root):
    """View user database connection."""
    target = f"`{PREFIX}%%`.*"
    credentials = dict(host=os.getenv("DJ_HOST"), user="djview", password="djview")
    permission = "SELECT"

    # Create MySQL users
    if version.parse(
        connection_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
        # create user if necessary on mysql8
        connection_root.query(
            f"""
            CREATE USER IF NOT EXISTS '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON {target}
            TO '{credentials["user"]}'@'%%';
            """
        )
    else:
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
        connection_root.query(
            f"""
            GRANT {permission} ON {target}
            TO '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )

    connection = dj.Connection(**credentials)
    yield connection
    connection_root.query(f"""DROP USER `{credentials["user"]}`""")
    connection.close()


def test_fail_create_schema(connection_view):
    """creating a schema with no CREATE privilege"""
    with pytest.raises(dj.DataJointError):
        return dj.Schema("forbidden_schema", locals(), connection=connection_view)


# def test_insert_failure(schema, Language, connection_view):
#     unprivileged = dj.Schema(schema.database, locals(), connection=connection_view)
#     context = dict()
#     unprivileged.spawn_missing_classes(context=context)
#     assert issubclass(context["Language"], dj.Lookup) and len(
#         context["Language"]()
#     ) == len(Language()), "failed to spawn missing classes"
#     with pytest.raises(dj.DataJointError):
#         context["Language"]().insert1(("Socrates", "Greek"))


# def test_failure_to_create_table(schema, connection_view):
#     unprivileged = dj.Schema(schema.database, locals(), connection=connection_view)

#     @unprivileged
#     class Try(dj.Manual):
#         definition = """  # should not matter really
#         id : int
#         ---
#         value : float
#         """

#     with pytest.raises(dj.DataJointError):
#         Try().insert1((1, 1.5))


@pytest.fixture
def connection_subset(connection_root, schema, Parent, Child, NoAccess, NoAccessAgain):
    """Subset user database connection."""

    schema.activate(
        f"{PREFIX}_pipeline",
        connection=connection_root,
        create_tables=True,
    )
    credentials = dict(host=os.getenv("DJ_HOST"), user="djsubset", password="djsubset")
    permission = "SELECT, INSERT, UPDATE, DELETE"

    # Create MySQL users
    if version.parse(
        connection_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
        # create user if necessary on mysql8
        connection_root.query(
            f"""
            CREATE USER IF NOT EXISTS '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON `{PREFIX}_pipeline`.`#parent`
            TO '{credentials["user"]}'@'%%';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON `{PREFIX}_pipeline`.`__child`
            TO '{credentials["user"]}'@'%%';
            """
        )
    else:
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
        connection_root.query(
            f"""
            GRANT {permission} ON `{PREFIX}_pipeline`.`#parent`
            TO '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON `{PREFIX}_pipeline`.`__child`
            TO '{credentials["user"]}'@'%%';
            """
        )

    connection = dj.Connection(**credentials)
    yield connection
    connection_root.query(f"""DROP USER `{credentials["user"]}`""")
    schema.drop()
    connection.close()


def test_populate_activate(connection_subset):
    pass
    # importlib.reload(pipeline)
    # pipeline.schema.activate(
    #     f"{PREFIX}_pipeline", create_schema=True, create_tables=False
    # )
    # pipeline.Child.populate()
    # assert pipeline.Child.progress(display=False)[0] == 0
