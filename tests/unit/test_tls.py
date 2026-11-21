"""
Unit tests for the ``use_tls`` connection option.

The adapters' parameter mapping is checked with the driver's connect function
replaced. The handshake checks run the real drivers against a minimal local
server that answers the initial protocol exchange without offering TLS.
Connection and dj.conn are checked with a mock adapter.
"""

import importlib
import socket
import ssl
import struct
import threading
import time
from pathlib import Path
from unittest import mock

import certifi
import pymysql
import pytest
from pymysql.constants import CLIENT, CR

from datajoint.adapters import MySQLAdapter, PostgreSQLAdapter
from datajoint.adapters import mysql as mysql_adapter
from datajoint.adapters import postgres as postgres_adapter
from datajoint.errors import DataJointError

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


def _mysql_client_flags(received):
    """Capability flags of the client's handshake response (first packet sent)."""
    return struct.unpack("<I", bytes(received[4:8]))[0]


def _postgres_server_without_tls(sock, received):
    """Answer the SSLRequest with 'N' (no SSL)."""
    received.extend(sock.recv(8))
    sock.sendall(b"N")


def _postgres_server_with_tls(sock, received):
    """Answer the SSLRequest with 'S' (SSL supported), then stop."""
    received.extend(sock.recv(8))
    sock.sendall(b"S")


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
        """Replace the PyMySQL entry points; return the mocks."""
        mocks = dict(
            plain=mock.Mock(name="pymysql.connect"),
            preferred=mock.Mock(name="TLSPreferredConnection"),
            required=mock.Mock(name="TLSRequiredConnection"),
        )
        mocks["required"].return_value.server_capabilities = CLIENT.SSL
        monkeypatch.setattr(mysql_adapter.client, "connect", mocks["plain"])
        monkeypatch.setattr(mysql_adapter, "_TLSPreferredConnection", mocks["preferred"])
        monkeypatch.setattr(mysql_adapter, "_TLSRequiredConnection", mocks["required"])
        return mocks

    @staticmethod
    def _assert_unverified_context(params):
        assert params["ssl_disabled"] is False
        context = params["ssl"]
        assert isinstance(context, ssl.SSLContext)
        assert context.verify_mode == ssl.CERT_NONE
        assert context.check_hostname is False

    def test_none_prefers_tls(self, connect):
        MySQLAdapter().connect(**CREDENTIALS, use_tls=None)
        connect["plain"].assert_not_called()
        connect["required"].assert_not_called()
        self._assert_unverified_context(connect["preferred"].call_args.kwargs)

    @pytest.mark.parametrize("use_tls", [True, {}])
    def test_true_requires_tls_without_verification(self, connect, use_tls):
        MySQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        connect["plain"].assert_not_called()
        connect["preferred"].assert_not_called()
        self._assert_unverified_context(connect["required"].call_args.kwargs)

    def test_dict_becomes_tls_context(self, connect):
        """A dict is turned into an SSLContext by DataJoint, not passed to PyMySQL as a dict."""
        MySQLAdapter().connect(**CREDENTIALS, use_tls={"ca": certifi.where()})
        params = connect["required"].call_args.kwargs
        assert params["ssl_disabled"] is False
        assert isinstance(params["ssl"], ssl.SSLContext)
        assert params["ssl"].verify_mode == ssl.CERT_REQUIRED
        assert params["ssl"].check_hostname is True

    @pytest.mark.parametrize("use_tls", [{"ssl_ca": "/certs/ca.pem"}, {"ca": certifi.where(), "verify": True}, {1: True}])
    def test_unknown_key_raises(self, connect, use_tls):
        """A misspelled key is not ignored: the connection would run without the intended check."""
        with pytest.raises(DataJointError, match="Unknown use_tls key") as excinfo:
            MySQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        assert "Accepted keys: ca, capath, cert, key, password, cipher, check_hostname, verify_mode." in str(excinfo.value)
        for mocked in connect.values():
            mocked.assert_not_called()

    def test_unknown_verify_mode_raises(self, connect):
        with pytest.raises(DataJointError, match="verify_mode"):
            MySQLAdapter().connect(**CREDENTIALS, use_tls={"ca": certifi.where(), "verify_mode": "strict"})
        connect["required"].assert_not_called()

    def test_false_disables_tls(self, connect):
        MySQLAdapter().connect(**CREDENTIALS, use_tls=False)
        connect["required"].assert_not_called()
        connect["preferred"].assert_not_called()
        assert connect["plain"].call_args.kwargs["ssl_disabled"] is True
        assert "ssl" not in connect["plain"].call_args.kwargs

    def test_required_tls_rejects_plaintext_connection(self, connect):
        """If the server ends up not offering TLS, the connection is closed and refused."""
        connect["required"].return_value.server_capabilities = 0
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            MySQLAdapter().connect(**CREDENTIALS, use_tls=True)
        assert excinfo.value.args[0] == CR.CR_SSL_CONNECTION_ERROR
        connect["required"].return_value.close.assert_called_once()

    def test_dict_ca_verifies_certificate(self, monkeypatch):
        """With ``ca``, the certificate and the host name are verified."""
        default_context = ssl.create_default_context()
        create_default_context = mock.Mock(return_value=default_context)
        monkeypatch.setattr(mysql_adapter.ssl, "create_default_context", create_default_context)
        context = mysql_adapter._tls_context({"ca": "/certs/ca.pem"})
        assert create_default_context.call_args.kwargs["cafile"] == "/certs/ca.pem"
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname is True

    @pytest.mark.parametrize(
        "use_tls, verify_mode, check_hostname",
        [
            ({"ca": None}, ssl.CERT_NONE, False),
            ({"check_hostname": True}, ssl.CERT_NONE, False),  # no CA: nothing to verify against
            ({"capath": "CAPATH"}, ssl.CERT_REQUIRED, True),
            ({"ca": "CA", "check_hostname": False}, ssl.CERT_REQUIRED, False),
            ({"ca": "CA", "check_hostname": False, "verify_mode": "optional"}, ssl.CERT_OPTIONAL, False),
            ({"ca": "CA", "check_hostname": False, "verify_mode": False}, ssl.CERT_NONE, False),
            ({"verify_mode": True}, ssl.CERT_REQUIRED, False),
            ({"verify_mode": "Required"}, ssl.CERT_REQUIRED, False),
            ({"verify_mode": ssl.CERT_REQUIRED}, ssl.CERT_REQUIRED, False),
        ],
    )
    def test_tls_context_matches_pymysql(self, use_tls, verify_mode, check_hostname):
        """The context is the one PyMySQL builds from the same dict."""
        use_tls = _with_ca_paths(use_tls)
        context = mysql_adapter._tls_context(use_tls)
        assert (context.verify_mode, context.check_hostname) == (verify_mode, check_hostname)
        assert not context.verify_flags & ssl.VERIFY_X509_STRICT
        if not isinstance(use_tls.get("verify_mode"), ssl.VerifyMode):  # PyMySQL ignores an SSL enum value
            expected = pymysql.connections.Connection._create_ssl_ctx(None, use_tls)
            assert (expected.verify_mode, expected.check_hostname) == (verify_mode, check_hostname)

    @pytest.mark.parametrize(
        "use_tls", [{}, {"ca": "CA"}, {"ca": "CA", "check_hostname": False}, {"capath": "CAPATH", "check_hostname": False}]
    )
    @pytest.mark.parametrize(
        "verify_mode, name", [(ssl.CERT_NONE, "none"), (ssl.CERT_OPTIONAL, "optional"), (ssl.CERT_REQUIRED, "required")]
    )
    def test_verify_mode_enum_means_its_name(self, use_tls, verify_mode, name):
        """
        An ``ssl.VerifyMode`` is applied as PyMySQL applies the matching name.

        PyMySQL ignored an enum value and used its default instead; DataJoint gives
        it the meaning PyMySQL gives 'none', 'optional' and 'required'. ``ssl``
        refuses CERT_NONE together with the host name check, for the name as for
        the enum value.
        """
        use_tls = _with_ca_paths(use_tls)
        try:
            expected = pymysql.connections.Connection._create_ssl_ctx(None, {**use_tls, "verify_mode": name})
        except ValueError:
            with pytest.raises(ValueError, match="check_hostname"):
                mysql_adapter._tls_context({**use_tls, "verify_mode": verify_mode})
            return
        context = mysql_adapter._tls_context({**use_tls, "verify_mode": verify_mode})
        assert (context.verify_mode, context.check_hostname) == (expected.verify_mode, expected.check_hostname)


def _with_ca_paths(use_tls):
    """Replace the placeholders "CA" and "CAPATH" with a real CA file and folder."""
    where = certifi.where()
    return {k: (where if v == "CA" else str(Path(where).parent) if v == "CAPATH" else v) for k, v in use_tls.items()}


class TestMySQLTLSHandshake:
    """Real PyMySQL against a server that does not offer TLS."""

    def test_authentication_hook_exists(self):
        """The TLS check overrides a PyMySQL method; fail loudly if it is renamed."""
        assert callable(getattr(pymysql.connections.Connection, "_request_authentication", None))

    @pytest.mark.parametrize("use_tls", [True, {}, {"ca": None, "check_hostname": False}])
    def test_required_tls_fails_without_sending_credentials(self, fake_server, use_tls):
        server = fake_server(_mysql_server_without_tls)
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            MySQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=use_tls)
        assert excinfo.value.args[0] == CR.CR_SSL_CONNECTION_ERROR
        server.close()
        assert len(server.received) == 1
        assert b"alice" not in bytes(server.received[0])

    def test_preferred_tls_continues_without_tls_in_one_connection(self, fake_server):
        """use_tls=None: the login proceeds without TLS on the same connection, without the TLS flag."""
        server = fake_server(_mysql_server_without_tls)
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            MySQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None)
        assert excinfo.value.args[0] == 1045  # the server rejected the login; not retried
        server.close()
        assert len(server.received) == 1
        assert b"alice" in bytes(server.received[0])
        assert not _mysql_client_flags(server.received[0]) & CLIENT.SSL

    def test_preferred_tls_handshake_failure_is_not_retried(self, fake_server):
        server = fake_server(_mysql_server_failing_handshake)
        with pytest.raises(pymysql.err.OperationalError):
            MySQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None)
        server.close()
        assert len(server.received) == 1
        assert b"alice" not in bytes(server.received[0])


@pytest.fixture(scope="module")
def certificates(tmp_path_factory):
    """A CA, and a server certificate it signed for the host name ``db.example`` only."""
    pytest.importorskip("cryptography")
    import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    def name(common_name):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])

    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key, server_key = ec.generate_private_key(ec.SECP256R1()), ec.generate_private_key(ec.SECP256R1())
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(name("Test CA"))
        .issuer_name(name("Test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(name("db.example"))
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("db.example")]), critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    folder = tmp_path_factory.mktemp("certs")
    pem = serialization.Encoding.PEM
    (folder / "ca.pem").write_bytes(ca_cert.public_bytes(pem))
    (folder / "server.pem").write_bytes(server_cert.public_bytes(pem))
    (folder / "server.key").write_bytes(
        server_key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return folder


def _mysql_server_with_tls(certificates, decrypted):
    """Greet with CLIENT_SSL, complete the TLS handshake, then reject the login sent over TLS."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificates / "server.pem", certificates / "server.key")

    def respond(sock, received):
        sock.sendall(_mysql_greeting(MYSQL_CAPABILITIES | CLIENT.SSL))
        request = sock.recv(36)  # SSL request packet: 4-byte header and 32-byte payload
        received.extend(request)
        try:
            tls = context.wrap_socket(sock, server_side=True)
        except ssl.SSLError:
            return  # the client refused the certificate
        with tls:
            decrypted.extend(tls.recv(4096))  # handshake response, with the user name
            error = b"\xff" + struct.pack("<H", 1045) + b"#28000" + b"Access denied"
            tls.sendall(_mysql_packet(error, 3))

    return respond


class TestMySQLTLSVerification:
    """Real PyMySQL through DataJoint's connection code, against a server that completes the TLS handshake."""

    def test_host_name_mismatch_is_refused_before_login(self, fake_server, certificates):
        decrypted = bytearray()
        server = fake_server(_mysql_server_with_tls(certificates, decrypted))
        with pytest.raises(pymysql.err.OperationalError, match="certificate verify failed"):
            MySQLAdapter().connect(
                host="127.0.0.1",
                port=server.port,
                user="alice",
                password="secret",
                use_tls={"ca": str(certificates / "ca.pem")},
            )
        server.close()
        assert b"alice" not in bytes(server.received[0])
        assert decrypted == b""

    def test_certificate_verified_without_host_name_check(self, fake_server, certificates):
        """check_hostname=False: the certificate is verified, the login goes over TLS only."""
        decrypted = bytearray()
        server = fake_server(_mysql_server_with_tls(certificates, decrypted))
        with pytest.raises(pymysql.err.OperationalError) as excinfo:
            MySQLAdapter().connect(
                host="127.0.0.1",
                port=server.port,
                user="alice",
                password="secret",
                use_tls={"ca": str(certificates / "ca.pem"), "check_hostname": False},
            )
        assert excinfo.value.args[0] == 1045  # the fake server's answer, received over TLS
        server.close()
        assert b"alice" in bytes(decrypted)
        assert b"alice" not in bytes(server.received[0])

    def test_untrusted_certificate_is_refused(self, fake_server, certificates):
        """A CA that did not sign the server certificate fails verification, even without the host name check."""
        decrypted = bytearray()
        server = fake_server(_mysql_server_with_tls(certificates, decrypted))
        with pytest.raises(pymysql.err.OperationalError, match="certificate verify failed"):
            MySQLAdapter().connect(
                host="127.0.0.1",
                port=server.port,
                user="alice",
                password="secret",
                use_tls={"ca": certifi.where(), "check_hostname": False},
            )
        server.close()
        assert decrypted == b""


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
            (None, dict(sslmode="require")),
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
        assert connect.call_count == 1
        params = connect.call_args.kwargs
        tls_params = {k: v for k, v in params.items() if k.startswith("ssl")}
        assert tls_params == expected

    def test_explicit_sslmode_takes_precedence(self, connect):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=True, sslmode="verify-full")
        assert connect.call_args.kwargs["sslmode"] == "verify-full"

    @pytest.mark.parametrize(
        "use_tls, reported",
        [
            ({"ca": "/certs/ca.pem", "cipher": "AES256"}, "cipher"),
            ({"check_hostname": True}, "check_hostname"),
            ({"verify_mode": True, "capath": "/certs"}, "capath, verify_mode"),
        ],
    )
    def test_unsupported_keys_are_reported(self, connect, caplog, use_tls, reported):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        assert f"ignored for PostgreSQL: {reported}." in caplog.text
        assert connect.call_args.kwargs["sslmode"] in ("require", "verify-full")

    def test_unknown_key_raises(self, connect):
        with pytest.raises(DataJointError, match="Unknown use_tls key.*'ssl_ca'.*Accepted keys: ca, capath"):
            PostgreSQLAdapter().connect(**CREDENTIALS, use_tls={"ssl_ca": "/certs/ca.pem"})
        connect.assert_not_called()

    def test_keys_requesting_nothing_are_not_reported(self, connect, caplog):
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls={"check_hostname": False, "verify_mode": None})
        assert "ignored" not in caplog.text


class TestPostgreSQLDefaultFallback:
    """use_tls=None: sslmode=require, then sslmode=disable only when SSL is refused before authentication."""

    @pytest.fixture
    def psycopg2(self):
        return pytest.importorskip("psycopg2")

    @pytest.fixture
    def connect(self, monkeypatch, psycopg2):
        connect = mock.Mock(name="psycopg2.connect")
        monkeypatch.setattr(psycopg2, "connect", connect)
        return connect

    @pytest.mark.parametrize(
        "message",
        [
            'connection to server at "db", port 5432 failed: server does not support SSL, but SSL was required',
            'connection to server at "db", port 5432 failed: FATAL:  no pg_hba.conf entry for host "10.0.0.1", '
            'user "alice", database "test", SSL encryption',
            'FATAL:  pg_hba.conf rejects connection for host "10.0.0.1", user "alice", database "test", SSL encryption',
            'FATAL:  no pg_hba.conf entry for host "10.0.0.1", user "alice", database "test", SSL on',
        ],
    )
    def test_falls_back_when_ssl_refused(self, connect, psycopg2, message):
        connect.side_effect = [psycopg2.OperationalError(message), mock.MagicMock()]
        PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=None)
        assert [c.kwargs["sslmode"] for c in connect.call_args_list] == ["require", "disable"]

    @pytest.mark.parametrize(
        "message",
        [
            'connection to server at "db", port 5432 failed: FATAL:  password authentication failed for user "alice"',
            'connection to server at "db", port 5432 failed: FATAL:  no pg_hba.conf entry for host "10.0.0.1", '
            'user "alice", database "test", no encryption',
            'connection to server at "db", port 5432 failed: SSL SYSCALL error: Connection reset by peer',
            'connection to server at "db", port 5432 failed: Connection refused',
        ],
    )
    def test_does_not_fall_back_after_other_errors(self, connect, psycopg2, message):
        connect.side_effect = psycopg2.OperationalError(message)
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=None)
        assert connect.call_count == 1

    # The same refusal as reported by a libpq built with another language
    LOCALIZED_REFUSAL = (
        'Verbindung zum Server auf "db", Port 5432 fehlgeschlagen: Server unterstützt kein SSL, aber SSL wurde verlangt'
    )

    def test_falls_back_when_server_answers_no_ssl_in_any_language(self, connect, psycopg2, fake_server):
        """Whether the server supports SSL is asked in the protocol, not read from the message."""
        server = fake_server(_postgres_server_without_tls)
        connect.side_effect = [psycopg2.OperationalError(self.LOCALIZED_REFUSAL), mock.MagicMock()]
        PostgreSQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None)
        assert [c.kwargs["sslmode"] for c in connect.call_args_list] == ["require", "disable"]
        server.close()
        assert [bytes(r) for r in server.received] == [struct.pack("!ii", 8, 80877103)]  # no credentials

    def test_does_not_fall_back_when_server_supports_ssl(self, connect, psycopg2, fake_server):
        server = fake_server(_postgres_server_with_tls)
        connect.side_effect = psycopg2.OperationalError("Passwort-Authentifizierung für Benutzer »alice« fehlgeschlagen")
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None)
        assert connect.call_count == 1

    @pytest.mark.parametrize("host", ["/var/run/postgresql", "db1,db2", ""])
    def test_does_not_probe_other_hosts(self, connect, psycopg2, monkeypatch, host):
        create_connection = mock.Mock(side_effect=AssertionError("probed"))
        monkeypatch.setattr(socket, "create_connection", create_connection)
        connect.side_effect = psycopg2.OperationalError(self.LOCALIZED_REFUSAL)
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(host=host, port=5432, user="alice", password="secret", use_tls=None)
        assert connect.call_count == 1
        create_connection.assert_not_called()

    def test_does_not_fall_back_when_server_unreachable(self, connect, psycopg2):
        listener = socket.create_server(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()  # nothing listens on this port
        connect.side_effect = psycopg2.OperationalError(self.LOCALIZED_REFUSAL)
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(host="127.0.0.1", port=port, user="alice", password="secret", use_tls=None)
        assert connect.call_count == 1

    @pytest.fixture
    def clock(self, monkeypatch):
        """The adapter's monotonic clock, advanced by hand."""
        clock = mock.Mock(now=100.0)
        clock.side_effect = lambda: clock.now
        monkeypatch.setattr(postgres_adapter, "monotonic", clock)
        return clock

    def test_no_ssl_check_after_first_attempt_timed_out(self, connect, psycopg2, monkeypatch, clock):
        """A first attempt that used all of connect_timeout did not reach the server: the time to fail is not doubled."""
        create_connection = mock.Mock(side_effect=AssertionError("probed"))
        monkeypatch.setattr(socket, "create_connection", create_connection)

        def time_out(**kwargs):
            clock.now += kwargs["connect_timeout"]
            raise psycopg2.OperationalError("Zeitüberschreitung beim Verbindungsaufbau")  # "timeout expired"

        connect.side_effect = time_out
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=None, connect_timeout=5)
        assert connect.call_count == 1
        create_connection.assert_not_called()

    @pytest.mark.parametrize(
        "connect_timeout, elapsed, budget",
        [(10, 4, 6), (5, 0.5, 4.5), (0, 3, 7)],  # connect_timeout=0 (no limit): 10 seconds
    )
    def test_ssl_check_gets_what_is_left_of_connect_timeout(
        self, connect, psycopg2, monkeypatch, clock, connect_timeout, elapsed, budget
    ):
        create_connection = mock.Mock(side_effect=socket.timeout("timed out"))
        monkeypatch.setattr(socket, "create_connection", create_connection)

        def refuse(**kwargs):
            clock.now += elapsed
            raise psycopg2.OperationalError(self.LOCALIZED_REFUSAL)

        connect.side_effect = refuse
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=None, connect_timeout=connect_timeout)
        assert connect.call_count == 1
        assert create_connection.call_args.kwargs["timeout"] == pytest.approx(budget)

    def test_ssl_check_against_silent_server_ends_with_connect_timeout(self, connect, psycopg2, fake_server):
        """A server that accepts the connection but never answers holds the check only until connect_timeout."""
        server = fake_server(lambda sock, received: received.extend(sock.recv(8)))

        def slow_refusal(**kwargs):
            time.sleep(0.6)
            raise psycopg2.OperationalError(self.LOCALIZED_REFUSAL)

        connect.side_effect = slow_refusal
        started = time.monotonic()
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(
                host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None, connect_timeout=1
            )
        assert time.monotonic() - started < 1.35  # 1 second, not 0.6 + 1
        assert connect.call_count == 1
        server.close()
        assert [bytes(r) for r in server.received] == [struct.pack("!ii", 8, 80877103)]

    @pytest.mark.parametrize("use_tls", [True, {"ca": "/certs/ca.pem"}])
    def test_does_not_fall_back_when_tls_requested(self, connect, psycopg2, use_tls):
        connect.side_effect = psycopg2.OperationalError("server does not support SSL, but SSL was required")
        with pytest.raises(psycopg2.OperationalError):
            PostgreSQLAdapter().connect(**CREDENTIALS, use_tls=use_tls)
        assert connect.call_count == 1


class TestPostgreSQLTLSHandshake:
    """Real libpq against a server that does not offer SSL."""

    @pytest.fixture(autouse=True)
    def psycopg2(self):
        return pytest.importorskip("psycopg2")

    def test_required_tls_fails_without_sending_credentials(self, fake_server, psycopg2):
        server = fake_server(_postgres_server_without_tls)
        with pytest.raises(psycopg2.OperationalError, match="does not support SSL"):
            PostgreSQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=True)
        server.close()
        assert len(server.received) == 1
        assert b"alice" not in bytes(server.received[0])

    def test_default_retries_without_ssl(self, fake_server, psycopg2):
        """use_tls=None: the SSL attempt sends no credentials; the second attempt is without SSL."""
        server = fake_server(_postgres_server_without_tls)
        with pytest.raises(psycopg2.OperationalError):  # the fake server cannot complete a login
            PostgreSQLAdapter().connect(host="127.0.0.1", port=server.port, user="alice", password="secret", use_tls=None)
        server.close()
        assert len(server.received) == 2
        assert b"alice" not in bytes(server.received[0])


# =============================================================================
# Connection and dj.conn
# =============================================================================


@pytest.fixture
def dj():
    """The datajoint package as currently imported (other tests may re-import it)."""
    return importlib.import_module("datajoint")


@pytest.fixture
def adapter(monkeypatch, dj):
    """Mock adapter returned by get_adapter for every Connection, with database.use_tls unset."""
    adapter = mock.Mock(name="adapter")
    adapter.backend = "postgresql"  # skips the MySQL server-version query
    adapter.get_connection_id.return_value = 1
    monkeypatch.setattr(importlib.import_module("datajoint.connection"), "get_adapter", lambda backend: adapter)
    with dj.config.override(database__use_tls=None):  # DJ_USE_TLS must not change the None cases
        yield adapter


def _use_tls_calls(adapter):
    return [c.kwargs["use_tls"] for c in adapter.connect.call_args_list]


class TestConnectionUseTLS:
    @pytest.mark.parametrize("use_tls", [None, True, False, {"ca": "/certs/ca.pem"}])
    def test_use_tls_reaches_adapter(self, dj, adapter, use_tls):
        dj.Connection("localhost", "alice", "secret", 5432, use_tls)
        assert _use_tls_calls(adapter) == [use_tls]

    def test_use_tls_from_config(self, dj, adapter):
        with dj.config.override(database__use_tls=True):
            dj.Connection("localhost", "alice", "secret", 5432)
        assert _use_tls_calls(adapter) == [True]

    def test_use_tls_argument_overrides_config(self, dj, adapter):
        with dj.config.override(database__use_tls=True):
            dj.Connection("localhost", "alice", "secret", 5432, False)
        assert _use_tls_calls(adapter) == [False]

    @pytest.mark.parametrize("use_tls", [True, False])
    def test_instance_use_tls_from_config(self, dj, adapter, use_tls):
        dj.Instance("localhost", "alice", "secret", database__use_tls=use_tls)
        assert _use_tls_calls(adapter) == [use_tls]

    def test_instance_use_tls_argument_overrides_config(self, dj, adapter):
        dj.Instance("localhost", "alice", "secret", use_tls=False, database__use_tls=True)
        assert _use_tls_calls(adapter) == [False]

    def test_reconnect_keeps_use_tls(self, dj, adapter):
        connection = dj.Connection("localhost", "alice", "secret", 5432, False)
        connection.connect()
        assert _use_tls_calls(adapter) == [False, False]

    def test_connection_error_is_not_retried(self, dj, adapter):
        adapter.connect.side_effect = pymysql.err.OperationalError(1045, "Access denied")
        with pytest.raises(pymysql.err.OperationalError):
            dj.Connection("localhost", "alice", "secret", 5432, None)
        assert adapter.connect.call_count == 1

    @pytest.mark.parametrize(
        "made_with, requested, reconnects",
        [
            (None, True, True),
            (False, True, True),
            (None, {"ca": "/certs/ca.pem"}, True),
            (True, {"ca": "/certs/ca.pem"}, True),
            ({"ca": "/certs/ca.pem"}, {"ca": "/certs/other.pem"}, True),
            (True, True, False),
            ({"ca": "/certs/ca.pem"}, True, False),
            ({"ca": "/certs/ca.pem"}, {"ca": "/certs/ca.pem"}, False),
            (True, None, False),
            (True, False, False),
            (None, False, False),
        ],
    )
    def test_ensure_tls(self, dj, adapter, made_with, requested, reconnects):
        connection = dj.Connection("localhost", "alice", "secret", 5432, made_with)
        connection._ensure_tls(requested)
        if reconnects:
            assert _use_tls_calls(adapter) == [made_with, requested]
            assert connection.conn_info["ssl_input"] == requested
        else:
            assert _use_tls_calls(adapter) == [made_with]
            assert connection.conn_info["ssl_input"] == made_with

    def test_ensure_tls_refused_during_transaction(self, dj, adapter):
        connection = dj.Connection("localhost", "alice", "secret", 5432, None)
        connection._in_transaction = True
        with pytest.raises(dj.errors.DataJointError):
            connection._ensure_tls(True)
        assert adapter.connect.call_count == 1


class TestConnUseTLS:
    """dj.conn and datajoint.connection.conn honor use_tls for the shared connection."""

    @pytest.fixture
    def singleton(self, monkeypatch, adapter):
        instance_module = importlib.import_module("datajoint.instance")
        monkeypatch.setattr(instance_module, "_singleton_connection", None)
        yield
        monkeypatch.setattr(instance_module, "_singleton_connection", None)

    def test_new_connection_without_credentials_uses_use_tls(self, dj, singleton, adapter):
        with dj.config.override(database__user="alice", database__password="secret", database__use_tls=None):
            connection = dj.conn(use_tls=True)
        assert _use_tls_calls(adapter) == [True]
        assert connection.conn_info["ssl_input"] is True

    def test_existing_connection_reconnects_only_when_more_tls_is_requested(self, dj, singleton, adapter):
        connection = dj.conn("localhost", "alice", "secret")
        assert dj.conn(use_tls=True) is connection
        assert dj.conn("localhost", "alice", "secret", use_tls={"ca": "/certs/ca.pem"}) is connection
        assert dj.conn(use_tls=True) is connection  # a dict already requires TLS
        assert dj.conn(use_tls=False) is connection
        assert _use_tls_calls(adapter) == [None, True, {"ca": "/certs/ca.pem"}]

    def test_connection_module_conn(self, monkeypatch, adapter):
        connection_module = importlib.import_module("datajoint.connection")
        monkeypatch.delattr(connection_module.conn, "connection", raising=False)
        connection = connection_module.conn("localhost", "alice", "secret", use_tls=None)
        assert connection_module.conn(use_tls=True) is connection
        assert _use_tls_calls(adapter) == [None, True]
        monkeypatch.delattr(connection_module.conn, "connection")
