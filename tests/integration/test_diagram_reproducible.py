"""
Byte-reproducibility of rendered diagrams across processes (#1551).

Python salts ``str.__hash__`` per process (PEP 456), so iterating a set of table
names yields a different order in every interpreter. Wherever such an order
reaches the renderer, two identical runs emit nodes, edges and clusters in a
different sequence and the output differs byte for byte, although the picture
is the same. That defeats committing generated figures and checking them for
drift. An in-process test cannot catch this, so the same diagram is rendered in
separate interpreters with different ``PYTHONHASHSEED`` values and compared.
"""

import json
import os
import subprocess
import sys
import time

import pytest

import datajoint as dj

# Declares a pipeline with several master-part groups and renders it three ways:
# the whole schema, a neighborhood that holds fewer than half of the loaded
# tables (networkx then iterates its subgraph in set order), and a partly
# collapsed diagram. Writes the dot source, SVG and Mermaid of each as JSON.
_SCRIPT = '''
import json
import sys

import datajoint as dj

cfg = json.loads(sys.argv[1])
dj.config["database.backend"] = cfg["backend"]
dj.config["database.host"] = cfg["host"]
dj.config["database.port"] = cfg["port"]
dj.config["database.user"] = cfg["user"]
dj.config["database.password"] = cfg["password"]

schema = dj.Schema(cfg["schema"])


@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int32
    """


@schema
class Rig(dj.Lookup):
    definition = """
    rig : varchar(16)
    """


@schema
class Session(dj.Manual):
    definition = """
    -> Subject
    session_id : int32
    ---
    -> Rig
    """

    class Note(dj.Part):
        definition = """
        -> master
        note_id : int32
        """

    class Stimulus(dj.Part):
        definition = """
        -> master
        stim_id : int32
        """


@schema
class Scan(dj.Imported):
    definition = """
    -> Session
    """

    class Field(dj.Part):
        definition = """
        -> master
        field_id : int32
        """

    class Channel(dj.Part):
        definition = """
        -> master
        channel : int32
        """


@schema
class Segmentation(dj.Computed):
    definition = """
    -> Scan
    """

    class Mask(dj.Part):
        definition = """
        -> master
        -> Scan.Field
        mask_id : int32
        """


@schema
class Fluorescence(dj.Computed):
    definition = """
    -> Segmentation
    """

    class Trace(dj.Part):
        definition = """
        -> master
        -> Segmentation.Mask
        """


@schema
class Pairing(dj.Manual):
    definition = """
    -> Session
    -> Session.proj(other_subject_id="subject_id", other_session_id="session_id")
    """


diagrams = {
    "schema": dj.Diagram(schema),
    "neighborhood": dj.Diagram(Scan) + 1,
    "collapsed": dj.Diagram(schema).collapse() + dj.Diagram(Segmentation),
}
rendered = {
    name: {"dot": d.make_dot().to_string(), "svg": d.svg_string(), "mermaid": d.make_mermaid()}
    for name, d in diagrams.items()
}
with open(cfg["out"], "w") as f:
    json.dump(rendered, f)
'''

# Seeds chosen so that, before the fix, set iteration order differed between them.
_SEEDS = ("0", "1", "12345")


@pytest.fixture(scope="function")
def schema_name_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_repro_{backend}_{test_id}"[:64]
    quoted = connection_by_backend.adapter.quote_identifier(schema_name)
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(f"DROP DATABASE IF EXISTS {quoted}")
        except Exception:
            pass
    dj.Schema(schema_name, connection=connection_by_backend)
    yield schema_name
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(f"DROP DATABASE IF EXISTS {quoted}")
        except Exception:
            pass


def _render(seed, cfg, tmp_path):
    out = tmp_path / f"render_{seed}.json"
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT, json.dumps(dict(cfg, out=str(out)))],
        env=dict(os.environ, PYTHONHASHSEED=seed),
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, f"render with PYTHONHASHSEED={seed} failed:\n{proc.stderr}"
    return json.loads(out.read_text())


def test_render_is_independent_of_hash_seed(schema_name_by_backend, db_creds_by_backend, tmp_path):
    host, port = db_creds_by_backend["host"].rsplit(":", 1)
    cfg = dict(
        backend=db_creds_by_backend["backend"],
        host=host,
        port=int(port),
        user=db_creds_by_backend["user"],
        password=db_creds_by_backend["password"],
        schema=schema_name_by_backend,
    )
    reference, *others = (_render(seed, cfg, tmp_path) for seed in _SEEDS)
    mismatches = [
        f"{name} diagram, {fmt} output, PYTHONHASHSEED={_SEEDS[0]} vs {seed}"
        for seed, rendered in zip(_SEEDS[1:], others)
        for name, outputs in reference.items()
        for fmt, text in outputs.items()
        if rendered[name][fmt] != text
    ]
    assert not mismatches, "rendering depends on the hash seed:\n" + "\n".join(mismatches)
