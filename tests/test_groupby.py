from .schema_simple import A, D


def test_aggr_with_proj(schema_simp):
    # issue #944 - only breaks with MariaDB
    # MariaDB implements the SQL:1992 standard that prohibits fields in the select statement that are
    # not also in the GROUP BY statement.
    # An improved specification in SQL:2003 allows fields that are functionally dependent on the group by
    # attributes to be allowed in the select. This behavior is implemented by MySQL correctly but not MariaDB yet.
    # See MariaDB issue: https://jira.mariadb.org/browse/MDEV-11588
    A.aggr(D.proj(m="id_l"), ..., n="max(m) - min(m)")
