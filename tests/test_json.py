from .schema_simple import Team
import inspect
from datajoint.declare import declare


def test_insert_update():
    car = {
        "name": "Discovery",
        "length": 22.9,
        "inspected": None,
        "tire_pressure": [35, 36, 34, 37],
        "headlights": [
            {
                "side": "left",
                "hyper_white": True,
            },
            {
                "side": "right",
                "hyper_white": True,
            },
        ],
    }

    Team.insert1({"name": "research", "car": car})
    q = Team & {"name": "research"}
    assert q.fetch1("car") == car

    car.update({"length": 23})
    Team.update1({"name": "research", "car": car})
    assert q.fetch1("car") == car

    q.delete_quick()
    assert not q


def test_describe():
    rel = Team()
    context = inspect.currentframe().f_globals
    s1 = declare(rel.full_table_name, rel.definition, context)
    s2 = declare(rel.full_table_name, rel.describe(), context)
    assert s1 == s2


def test_query():
    # dict
    assert (Team & {"car.name": "Chaching"}).fetch1("name") == "business"
    assert (Team & {"car.length": 20.5}).fetch1("name") == "engineering"
    assert (Team & {"car.inspected": "true"}).fetch1("name") == "engineering"
    assert (Team & {"car.inspected:unsigned": True}).fetch1("name") == "engineering"
    assert (Team & {"car.safety_inspected": "false"}).fetch1("name") == "business"
    assert (Team & {"car.safety_inspected:unsigned": False}).fetch1(
        "name"
    ) == "business"
    assert (Team & {"car.headlights[0].hyper_white": None}).fetch(
        "name", order_by="name", as_dict=True
    ) == [
        {"name": "engineering"},
        {"name": "marketing"},
    ]  # if entire record missing, JSON key is missing, or value set to JSON null
    assert (Team & {"car": None}).fetch1("name") == "marketing"
    assert (Team & {"car.tire_pressure": [34, 30, 27, 32]}).fetch1("name") == "business"
    assert (
        Team & {"car.headlights[1]": {"side": "right", "hyper_white": True}}
    ).fetch1("name") == "business"
    # sql operators
    assert (Team & "`car`->>'$.name' LIKE '%ching%'").fetch1(
        "name"
    ) == "business", "Missing substring"
    assert (Team & "`car`->>'$.length' > 30").fetch1("name") == "business", "<= 30"
    assert (
        Team & "JSON_VALUE(`car`, '$.safety_inspected' RETURNING UNSIGNED) = 0"
    ).fetch1("name") == "business", "Has `safety_inspected` set to `true`"
    assert (Team & "`car`->>'$.headlights[0].hyper_white' = 'null'").fetch1(
        "name"
    ) == "engineering", "Has 1st `headlight` with `hyper_white` not set to `null`"
    assert (Team & "`car`->>'$.inspected' IS NOT NULL").fetch1(
        "name"
    ) == "engineering", "Missing `inspected` key"
    assert (Team & "`car`->>'$.tire_pressure' = '[34, 30, 27, 32]'").fetch1(
        "name"
    ) == "business", "`tire_pressure` array did not match"
    assert (
        Team
        & """`car`->>'$.headlights[1]' = '{"side": "right", "hyper_white": true}'"""
    ).fetch1("name") == "business", "2nd `headlight` object did not match"
