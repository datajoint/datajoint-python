from .schema_simple import A, D


def test_aggr_with_proj():
    # issue #944 - only breaks with MariaDB
    # Maria db wants all the fields in the select statement to be in the group by 
    # even if the fields are in fuctional dependency, this is incorrect and MySQL 
    # implements this properly
    # More info on the issue page
    A.aggr(D.proj(m='id_l'), ..., n='max(m) - min(m)')
