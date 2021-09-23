from .schema_simple import A, D


def test_aggr_with_proj():
    # issue #944 - only breaks with MariaDB
    A.aggr(D.proj(m='id_l'), ..., n='max(m) - min(m)')
