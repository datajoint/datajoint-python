import inspect
from datajoint.declare import declare
import datajoint as dj
import numpy as np
from distutils.version import LooseVersion
from . import PREFIX

if LooseVersion(dj.conn().query("select @@version;").fetchone()[0]) >= LooseVersion(
    "8.0.0"
):
    schema = dj.Schema(PREFIX + "_json")
    Team = None

    def setup():
        global Team

        @schema
        class Team(dj.Lookup):
            definition = """
            name: varchar(40)
            ---
            car=null: json
            unique index(car.name:char(20))
            uniQue inDex ( name, car.name:char(20), (json_value(`car`, _utf8mb4'$.length' returning decimal(4, 1))) )
            """
            contents = [
                (
                    "engineering",
                    {
                        "name": "Rever",
                        "length": 20.5,
                        "inspected": True,
                        "tire_pressure": [32, 31, 33, 34],
                        "headlights": [
                            {
                                "side": "left",
                                "hyper_white": None,
                            },
                            {
                                "side": "right",
                                "hyper_white": None,
                            },
                        ],
                    },
                ),
                (
                    "business",
                    {
                        "name": "Chaching",
                        "length": 100,
                        "safety_inspected": False,
                        "tire_pressure": [34, 30, 27, 32],
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
                    },
                ),
                (
                    "marketing",
                    None,
                ),
            ]

    def teardown():
        schema.drop()

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

        try:
            Team.insert1({"name": "hr", "car": car})
            raise Exception("Inserted non-unique car name.")
        except dj.DataJointError:
            pass

        q.delete_quick()
        assert not q

    def test_describe():
        rel = Team()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert s1 == s2

    def test_restrict():
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

        assert (Team & {"car.tire_pressure": [34, 30, 27, 32]}).fetch1(
            "name"
        ) == "business"

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

    def test_proj():
        # proj necessary since we need to rename indexed value into a proper attribute name
        assert Team.proj(car_length="car.length").fetch(
            as_dict=True, order_by="car_length"
        ) == [
            {"name": "marketing", "car_length": None},
            {"name": "business", "car_length": "100"},
            {"name": "engineering", "car_length": "20.5"},
        ]

        assert Team.proj(car_length="car.length:decimal(4, 1)").fetch(
            as_dict=True, order_by="car_length"
        ) == [
            {"name": "marketing", "car_length": None},
            {"name": "engineering", "car_length": 20.5},
            {"name": "business", "car_length": 100.0},
        ]

        assert Team.proj(
            car_width="JSON_VALUE(`car`, '$.length' RETURNING float) - 15"
        ).fetch(as_dict=True, order_by="car_width") == [
            {"name": "marketing", "car_width": None},
            {"name": "engineering", "car_width": 5.5},
            {"name": "business", "car_width": 85.0},
        ]

        assert (
            (Team & {"name": "engineering"}).proj(car_tire_pressure="car.tire_pressure")
        ).fetch1("car_tire_pressure") == "[32, 31, 33, 34]"

        assert np.array_equal(
            Team.proj(car_inspected="car.inspected").fetch(
                "car_inspected", order_by="name"
            ),
            np.array([None, "true", None]),
        )

        assert np.array_equal(
            Team.proj(car_inspected="car.inspected:unsigned").fetch(
                "car_inspected", order_by="name"
            ),
            np.array([None, 1, None]),
        )
