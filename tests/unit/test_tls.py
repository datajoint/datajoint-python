"""
Unit tests for the ``use_tls`` connection option.

The adapters' parameter mapping is checked with the driver's connect function
replaced. The handshake checks run the real drivers against a minimal local
server that answers the initial protocol exchange without offering TLS.
"""

import socket
import ssl
import struct
import threading
from unittest import mock

import pymysql
import pytest
from pymysql.constants import CLIENT, CR

from datajoint.adapters import MySQLAdapter, PostgreSQLAdapter
from datajoint.adapters import mysql as mysql_adapter
from datajoint.connection import Connection

CREDENTIALS = dict(host="localhost", port=3306, user="alice", password="secret")


# =============================================================================
# Minimal servers for the initial protocol exchange
# =============================================================================


class FakeServer:
    """Accept connections on localhost and answer each with ``respond(sock, received)``."""

    def __init__(self, respond):
        self.respond = respond
        self.received = []  # bytes received on each connection
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.listener.settimeout(0.05)
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        while not self.stopped.is_set():
            try:
                sock, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            received = bytearray()
            self.received.append(received)
            with sock:
                sock.settimeout(2)
                try:
                    self.respond(sock, received)
                    while chunk := sock.recv(4096):
                        received.extend(chunk)
                except OSError:
                    pass

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=5)
        self.listener.close()


def _mysql_packet(payload, seq):
    return struct.pack("<I", len(payload))[:3] + bytes([seq]) + payload


def _mysql_greeting(capabilities):
    """MySQL protocol v10 initial handshake packet."""
    salt = b"abcdefghijklmnopqrst"
    payload = (
        b"\x0a"
        + b"8.0.0-test\x00"
        + struct.pack("<I", 1)
        + salt[:8]
        + b"\x00"
        + struct.pack("<H", capabilities & 0xFFFF)
        + bytes([45])  # utf8mb4
        + struct.pack("<H", 2)  # autocommit
        + struct.pack("<H", capabilities >> 16)
        + bytes([len(salt) + 1])
        + b"\x00" * 10
        + salt[8:]
        + b"\x00"
        + b"mysql_native_password\x00"
    )
    return _mysql_packet(payload, 0)


MYSQL_CAPABILITIES = CLIENT.LONG_PASSWORD | CLIENT.PROTOCOL_41 | CLIENT.SECURE_CONNECTION | CLIENT.PLUGIN_AUTH


def _mysql_server_without_tls(sock, received):
    """Greet without CLIENT_SSL; reject any authentication attempt."""
    sock.sendall(_mysql_greeting(MYSQL_CAPABILITIES))
    chunk = sock.recv(4096)
    received.extend(chunk)
    if chunk:
        error = b"\xff" + struct.pack("<H", 1045) + b"#28000" + b"Access denied"
        sock.sendall(_mysql_packet(error, 2))


def _mysql_server_failing_handshake(sock, received):
    """Greet with CLIENT_SSL, then answer the TLS handshake with garbage."""
    sock.sendall(_mysql_greeting(MYSQL_CAPABILITIES | CLIENT.SSL))
    received.extend(sock.recv(4096))  # SSL request packet
    received.extend(sock.recv(4096))  # TLS ClientHello
    sock.sendall(b"\x00" * 64)


def _postgres_server_without_tls(sock, received):
    """Answer the SSLRequest with 'N' (no SSL)."""
    received.extend(sock.recv(8))
    sock.sendall(b"N")


@pytest.fixture
def fake_server():
    servers = []

    def start(respond):
        server = FakeServer(respond)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.close()


# =============================================================================
# MySQL
# =============================================================================


class TestMySQLTLSParameters:
    """use_tls → PyMySQL connection parameters."""

    @pytest.fixture
    def connect(self, monkeypatch):
        """Replace both PyMySQL entry points; return the mocks."""
        plain = mock.Mock(name="pymysql.connect")
        required = mock.Mock(name="TLSRequiredConnection")
        required.return_value.server_capabilities = CLIENT.SSL
        monkeypatch.setattr(mysql_adapter.client, "connect", plain)
        monkeypatch.setattr(mysql_adapter, "_TLSRequiredConnection", required)
        return plain, required

    @pytest.mark.parametrize("use_tls", [True, {}])
    def test_true_requires_tls_without_verification(self, connect, use_tls):
        plain, required = connect
        MySQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        plain.assert_not_called()
        params = required.call_args.kwargs
        assert params["ssl_disabled"] is False
        context = params["ssl"]
        assert isinstance(context, ssl.SSLContext)
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

    def test_dict_is_passed_to_pymysql(self, connect):
        plain, required = connect
        tls = {"ca": "/certs/ca.pem", "cert": "/certs/client.pem", "key": "/certs/client.key"}
        MySQLAdapter().connect(**CREDENTIALS, use_tls=tls)
        plain.assert_not_called()
        assert required.call_args.kwargs["ssl"] == tls
        assert required.call_args.kwargs["ssl_disabled"] is False

    def test_false_disables_tls(self, connect):
        plain, required = connect
        MySQLAdapter().connect(**CREDENTIALS, use_tls=False)
        required.assert_not_called()
        assert plain.call_args.kwargs["ssl_disabled"] is True
        assert "ssl" not in plain.call_args.kwargs

    def test_required_tls_rejects_plaintext_connection(self, connect):
        """If the server ends up not offering TLS, the connection is closed and refused."""
        _, required = connect
        required.return_value.server_capabilities = 0
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            MySQLAdapter().connect(**CREDENTIALS, use_tls=True)
        assert excinfo.value.args[0] == CR.CR_SSL_CONNECTION_ERROR
        required.return_value.close.assert_called_once()

    def test_dict_ca_verifies_certificate(self, monkeypatch):
        """PyMySQL verifies the certificate and host name for a dict with ``ca``."""
        default_context = ssl.create_default_context()
        create_default_context = mock.Mock(return_value=default_context)
        monkeypatch.setattr(pymysql.connections.ssl, "create_default_context", create_default_context)
        context = pymysql.connections.Connection._create_ssl_ctx(None, {"ca": "/certs/ca.pem"})
        assert create_default_context.call_args.kwargs["cafile"] == "/certs/ca.pem"
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True


class TestMySQLTLSHandshake:
    """Real PyMySQL against a server that does not offer TLS."""

    def test_authentication_hook_exists(self):
        """The TLS check overrides a PyMySQL method; fail loudly if it is renamed."""
        assert callable(getattr(pymysql.connections.Connection, "_request_authentication", None))

    @pytest.mark.parametrize("use_tls", [True, {}, {"ca": None, "check_hostname": False}])
    def test_required_tls_fails_without_sending_credentials(self, fake_server, use_tls):
        server = fake_server(_mysql_server_without_tls)
        adapter = MySQLAdapter()
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            adapter.connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=use_tls)
        assert excinfo.value.args[0] == CR.CR_SSL_CONNECTION_ERROR
        assert adapter.is_tls_negotiation_error(excinfo.value)
        server.close()
        assert len(server.received) == 1
        assert b"alice" not in bytes(server.received[0])

    def test_handshake_failure_is_tls_negotiation_error(self, fake_server):
        server = fake_server(_mysql_server_failing_handshake)
        adapter = MySQLAdapter()
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            adapter.connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=True)
        assert adapter.is_tls_negotiation_error(excinfo.value)
        server.close()
        assert b"alice" not in bytes(server.received[0])

    def test_authentication_error_is_not_tls_negotiation_error(self, fake_server):
        server = fake_server(_mysql_server_without_tls)
        adapter = MySQLAdapter()
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            adapter.connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=False)
        assert excinfo.value.args[0] == 1045
        assert not adapter.is_tls_negotiation_error(excinfo.value)


# =============================================================================
# PostgreSQL
# =============================================================================


class TestPostgreSQLTLSParameters:
    """use_tls → libpq connection parameters."""

    @pytest.fixture
    def connect(self, monkeypatch):
        psycopg2 = pytest.importorskip("psycopg2")
        connect = mock.Mock(name="psycopg2.connect")
        monkeypatch.setattr(psycopg2, "connect", connect)
        return connect

    @pytest.mark.parametrize(
        "use_tls, expected",
        [
            (None, dict(sslmode="prefer")),
            (False, dict(sslmode="disable")),
            (True, dict(sslmode="require")),
            ({}, dict(sslmode="require")),
            (
                {"ca": "/certs/ca.pem"},
                dict(sslmode="verify-full", sslrootcert="/certs/ca.pem"),
            ),
            (
                {"ca": "/certs/ca.pem", "check_hostname": False},
                dict(sslmode="verify-ca", sslrootcert="/certs/ca.pem"),
            ),
            (
                {"ca": "/certs/ca.pem", "cert": "/certs/client.pem", "key": "/certs/client.key", "password": "pw"},
                dict(
                    sslmode="verify-full",
                    sslrootcert="/certs/ca.pem",
                    sslcert="/certs/client.pem",
                    sslkey="/certs/client.key",
                    sslpassword="pw",
                ),
            ),
            (
                {"cert": "/certs/client.pem", "key": "/certs/client.key"},
                dict(sslmode="require", sslcert="/certs/client.pem", sslkey="/certs/client.key"),
            ),
        ],
    )
    def test_mapping(self, connect, use_tls, expected):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        params = connect.call_args.kwargs
        tls_params = {k: v for k, v in params.items() if k.startswith("ssl")}
        assert tls_params == expected

    def test_explicit_sslmode_takes_precedence(self, connect):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=True, sslmode="verify-full")
        assert connect.call_args.kwargs["sslmode"] == "verify-full"

    def test_unsupported_keys_are_reported(self, connect, caplog):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls={"ca": "/certs/ca.pem", "cipher": "AES256"})
        assert "cipher" in caplog.text
        assert "cipher" not in connect.call_args.kwargs


class TestPostgreSQLTLSHandshake:
    """Real libpq against a server that does not offer SSL."""

    @pytest.fixture(autouse=True)
    def psycopg2(self):
        return pytest.importorskip("psycopg2")

    def test_required_tls_fails_without_sending_credentials(self, fake_server, psycopg2):
        server = fake_server(_postgres_server_without_tls)
        adapter = PostgreSQLAdapter()
        with pytest.raises(psycopg2.OperationalError) as excinfo:
            adapter.connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=True)
        assert adapter.is_tls_negotiation_error(excinfo.value)
        server.close()
        assert len(server.received) == 1
        assert b"alice" not in bytes(server.received[0])

    def test_authentication_error_is_not_tls_negotiation_error(self, psycopg2):
        error = psycopg2.OperationalError('FATAL:  password authentication failed for user "alice"')
        assert not PostgreSQLAdapter().is_tls_negotiation_error(error)


# =============================================================================
# Fallback without TLS when use_tls is not set
# =============================================================================


def _connection(adapter, use_tls):
    """A Connection with its adapter set, before connect() is called."""
    connection = Connection.__new__(Connection)
    connection._config = {"connection.charset": ""}
    connection.conn_info = dict(host="localhost", port=3306, user="alice", passwd="secret", database_name=None)
    if use_tls is not False:
        connection.conn_info["ssl"] = use_tls if isinstance(use_tls, dict) else True
    connection.conn_info["ssl_input"] = use_tls
    connection.adapter = adapter
    return connection


class TestFallbackWithoutTLS:
    @pytest.fixture
    def adapter(self):
        adapter = mock.Mock(spec=MySQLAdapter)
        adapter.is_tls_negotiation_error.side_effect = lambda e: MySQLAdapter().is_tls_negotiation_error(e)
        return adapter

    def test_retries_when_server_does_not_support_tls(self, adapter, caplog):
        adapter.connect.side_effect = [mysql_adapter._TLSUnavailableError(), mock.sentinel.conn]
        connection = _connection(adapter, None)
        connection.connect()
        assert connection._conn is mock.sentinel.conn
        assert [c.kwargs["use_tls"] for c in adapter.connect.call_args_list] == [True, False]
        assert "Falling back to non-SSL" in caplog.text

    def test_does_not_retry_after_authentication_error(self, adapter):
        adapter.connect.side_effect = pymysql.err.OperationalError(1045, "Access denied")
        connection = _connection(adapter, None)
        with pytest.raises(pymysql.err.OperationalError):
            connection.connect()
        assert adapter.connect.call_count == 1

    @pytest.mark.parametrize("use_tls", [True, {"ca": "/certs/ca.pem"}])
    def test_does_not_retry_when_tls_requested(self, adapter, use_tls):
        adapter.connect.side_effect = mysql_adapter._TLSUnavailableError()
        connection = _connection(adapter, use_tls)
        with pytest.raises(pymysql.err.OperationalError):
            connection.connect()
        assert adapter.connect.call_count == 1

    def test_end_to_end_server_without_tls(self, fake_server):
        """Default use_tls: one TLS attempt (no credentials sent), then one attempt without TLS."""
        server = fake_server(_mysql_server_without_tls)
        connection = _connection(MySQLAdapter(), None)
        connection.conn_info.update(host="127.0.0.1", port=server.port)
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            connection.connect()
        assert excinfo.value.args[0] == 1045  # the server rejected the plaintext login
        server.close()
        assert len(server.received) == 2
        assert b"alice" not in bytes(server.received[0])
        assert b"alice" in bytes(server.received[1])
