import base64
from datetime import datetime, timezone
import json
import logging
import os
import sys
import flask
import webbrowser
import urllib
import http.client
import botocore
import botocore.config
from .log import log
from time import time
import multiprocessing

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from djsciops import settings as djsciops_settings
try:
    # Python 3.4+
    if sys.platform.startswith('win'):
        import multiprocessing.popen_spawn_win32 as forking
    else:
        import multiprocessing.popen_fork as forking
except ImportError:
    import multiprocessing.forking as forking

LOOKUP_SERVICE_ALLOWED_ORIGIN = "https://ops.datajoint.io"
LOOKUP_SERVICE_DOMAIN = "ops.datajoint.io"
LOOKUP_SERVICE_ROUTE = "/social-login/api/user"
# Everything LOOKUP_SERVICE_AUTH is changed, need to change:
# https://github.com/datajoint-company/dj-gitops/blob/main/applications/k8s/deployments/ops.datajoint.io/social_login_interceptor/client_store_secrets.yaml
LOOKUP_SERVICE_AUTH = {
    "https://accounts.datajoint.io/auth/": {
        "PROVIDER": "accounts.datajoint.io",
        "ROUTE": "/auth",
    },
    "https://accounts.datajoint.com/realms/datajoint": {
        "PROVIDER": "accounts.datajoint.com",
        "ROUTE": "/realms/datajoint/protocol/openid-connect",
    },
    "https://keycloak-qa.datajoint.io/realms/datajoint": {
        "PROVIDER": "keycloak-qa.datajoint.io",
        "ROUTE": "/realms/datajoint/protocol/openid-connect",
    },
}
issuer = djsciops_settings.get_config()["djauth"]["issuer"]


def _client_login(
    auth_client_id: str,
    auth_client_secret: str,
    auth_provider_domain: str = LOOKUP_SERVICE_AUTH[issuer]["PROVIDER"],
    auth_provider_token_route: str = f"{LOOKUP_SERVICE_AUTH[issuer]['ROUTE']}/token",
):
    connection = http.client.HTTPSConnection(auth_provider_domain)
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": auth_client_id,
            "client_secret": auth_client_secret,
        }
    )
    connection.request("POST", auth_provider_token_route, body, headers)
    jwt_payload = json.loads(connection.getresponse().read().decode())
    return jwt_payload["access_token"]


def start_server(q: multiprocessing.Queue, callback_port: int):
    """
    Starts Flask HTTP server.
    Since werkzeug 2.0.3 has vulnerability issue, has to upgrade and
    werkzeug.environ.shutdown_server() is deprecated after 2.0.3.
    """
    app = flask.Flask("browser-interface")

    @app.route("/login-cancelled")
    def login_cancelled():
        """
        Accepts requests which will cancel the user login.
        """
        q.put({"cancelled": True, "code": None})
        return """
            <!doctype html>
            <html>
            <head>
                <script>
                window.onload = function load() {
                window.open('', '_self', '');
                window.close();
                };
                </script>
            </head>
            <body>
            </body>
            </html>
            """

    @app.route("/login-completed")
    def login_completed():
        """
        Redirect after user has successfully logged in.
        """
        code = flask.request.args.get("code")
        q.put({"cancelled": False, "code": code})
        return """
            <!doctype html>
            <html>
            <head>
                <script>
                window.onload = function load() {
                window.open('', '_self', '');
                window.close();
                };
                </script>
            </head>
            <body>DataJoint login completed! Feel free to close this tab if it did not close automatically.</body>
            </html>
            """

    app.run(host="0.0.0.0", port=callback_port, debug=False)


def _oidc_login(
    auth_client_id: str,
    auth_url: str = f"https://{LOOKUP_SERVICE_AUTH[issuer]['PROVIDER']}{LOOKUP_SERVICE_AUTH[issuer]['ROUTE']}/auth",
    lookup_service_allowed_origin: str = LOOKUP_SERVICE_ALLOWED_ORIGIN,
    lookup_service_domain: str = LOOKUP_SERVICE_DOMAIN,
    lookup_service_route: str = LOOKUP_SERVICE_ROUTE,
    lookup_service_auth_provider: str = LOOKUP_SERVICE_AUTH[issuer]["PROVIDER"],
    code_challenge: str = "ubNp9Y0Y_FOENQ_Pz3zppyv2yyt0XtJsaPqUgGW9heA",
    code_challenge_method: str = "S256",
    code_verifier: str = "kFn5ZwL6ggOwU1OzKx0E1oZibIMC1ZbMC1WEUXcCV5mFoi015I9nB9CrgUJRkc3oiQT8uBbrvRvVzahM8OS0xJ51XdYaTdAlFeHsb6OZuBPmLD400ozVPrwCE192rtqI",
    callback_port: int = 28282,
    delay_seconds: int = 60,
):
    """
    Primary OIDC login flow.
    """

    # Prepare user
    log.warning(
        "User authentication required to use DataJoint SciOps CLI tools. We'll be "
        "launching a web browser to authenticate your DataJoint account."
    )
    # allocate variables for access and context
    code = None
    cancelled = True
    # Prepare HTTP server to communicate with browser
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    q = multiprocessing.Queue()
    server = multiprocessing.Process(
        target=start_server,
        args=(
            q,
            callback_port,
        ),
    )
    server.start()
    # build url
    query_params = dict(
        scope="openid",
        response_type="code",
        client_id=auth_client_id,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        redirect_uri=f"http://localhost:{callback_port}/login-completed",
    )
    link = f"{auth_url}?{urllib.parse.urlencode(query_params)}"
    # attempt to launch browser or provide instructions
    browser_available = True
    try:
        webbrowser.get()
    except webbrowser.Error:
        browser_available = False
    if browser_available:
        log.info("Browser available. Launching...")
        webbrowser.open(link, new=2)
    else:
        log.warning(
            "Browser unavailable. On a browser client, please navigate to the "
            f"following link to login: {link}"
        )
    # cancel_process = multiprocessing.Process(
    #     target=_delayed_request,
    #     kwargs=dict(
    #         url=f"http://localhost:{callback_port}/login-cancelled",
    #         delay=delay_seconds,
    #     ),
    # )
    # # cancel_process.start()
    queue_in_flask = q.get(block=True)
    cancelled = queue_in_flask["cancelled"]
    code = queue_in_flask["code"]
    # server.terminate()
    # cancel_process.terminate()
    # received a response
    if cancelled:
        server.terminate()
        raise Exception(
            "User login cancelled. User must be logged in to use DataJoint SciOps CLI tools."
        )
    else:
        # generate user info
        connection = http.client.HTTPSConnection(lookup_service_domain)
        headers = {
            "Content-type": "application/json",
            "Origin": lookup_service_allowed_origin,
        }
        body = json.dumps(
            {
                "auth_provider": lookup_service_auth_provider,
                "redirect_uri": f"http://localhost:{callback_port}/login-completed",
                "code_verifier": code_verifier,
                "client_id": auth_client_id,
                "code": code,
            }
        )
        connection.request("POST", lookup_service_route, body, headers)
        response = connection.getresponse().read().decode()
        try:
            userdata = json.loads(response)
            log.info("User successfully authenticated.")
            return (
                userdata["access_token"],
                userdata["username"],
                userdata["refresh_token"],
            )
        except json.decoder.JSONDecodeError:
            log.error(response)
            raise Exception("Login failed")
        finally:
            server.terminate()


def _delayed_request(*, url: str, delay: str = 0):
    time.sleep(delay)
    return urllib.request.urlopen(url)


def _decode_bearer_token(bearer_token):
    log.debug(f"bearer_token: {bearer_token}")
    jwt_data = json.loads(
        base64.b64decode((bearer_token.split(".")[1] + "==").encode()).decode()
    )
    log.debug(f"jwt_data: {jwt_data}")
    return jwt_data


if sys.platform.startswith('win'):
    # First define a modified version of Popen.
    class _Popen(forking.Popen):
        def __init__(self, *args, **kw):
            if hasattr(sys, 'frozen'):
                # We have to set original _MEIPASS2 value from sys._MEIPASS
                # to get --onefile mode working.
                os.putenv('_MEIPASS2', sys._MEIPASS)
            try:
                super(_Popen, self).__init__(*args, **kw)
            finally:
                if hasattr(sys, 'frozen'):
                    # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                    # available. In those cases we cannot delete the variable
                    # but only set it to the empty string. The bootloader
                    # can handle this case.
                    if hasattr(os, 'unsetenv'):
                        os.unsetenv('_MEIPASS2')
                    else:
                        os.putenv('_MEIPASS2', '')

    # Second override 'Popen' class with our modified version.
    forking.Popen = _Popen


class Session:
    def __init__(
        self,
        aws_account_id: str,
        s3_role: str,
        auth_client_id: str,
        auth_client_secret: str = None,
        bearer_token: str = None,
    ):
        self.aws_account_id = aws_account_id
        self.s3_role = s3_role
        self.auth_client_id = auth_client_id
        self.auth_client_secret = auth_client_secret
        self.sts_arn = f"arn:aws:iam::{aws_account_id}:role/{s3_role}"
        self.user = "client_credentials"
        self.refresh_token = None
        self.jwt = None
        # OAuth2.0 authorization
        if auth_client_secret:
            self.bearer_token = _client_login(
                auth_client_id=self.auth_client_id,
                auth_client_secret=self.auth_client_secret,
            )
            self.jwt = _decode_bearer_token(self.bearer_token)
        elif not bearer_token:
            self.bearer_token, self.user, self.refresh_token = _oidc_login(
                auth_client_id=auth_client_id,
            )
            self.jwt = _decode_bearer_token(self.bearer_token)
        else:
            self.jwt = _decode_bearer_token(self.bearer_token)
            time_to_live = (self.jwt["exp"] - datetime.utcnow().timestamp()) / 60 / 60
            log.info(
                f"Reusing provided bearer token with a life of {time_to_live} [HR]"
            )
            self.bearer_token, self.user = (bearer_token, self.jwt["sub"])

        self.sts_token = RefreshableBotoSession(session=self).refreshable_session()
        self.s3 = self.sts_token.resource(
            "s3", config=botocore.config.Config(s3={"use_accelerate_endpoint": True})
        )

    def refresh_bearer_token(
        self,
        lookup_service_allowed_origin: str = LOOKUP_SERVICE_ALLOWED_ORIGIN,
        lookup_service_domain: str = LOOKUP_SERVICE_DOMAIN,
        lookup_service_route: str = LOOKUP_SERVICE_ROUTE,
        lookup_service_auth_provider: str = LOOKUP_SERVICE_AUTH[issuer]["PROVIDER"],
    ):
        if self.auth_client_secret:
            self.bearer_token = _client_login(
                auth_client_id=self.auth_client_id,
                auth_client_secret=self.auth_client_secret,
            )
            self.jwt = _decode_bearer_token(self.bearer_token)
        else:
            # generate user info
            connection = http.client.HTTPSConnection(lookup_service_domain)
            headers = {
                "Content-type": "application/json",
                "Origin": lookup_service_allowed_origin,
            }
            body = json.dumps(
                {
                    "auth_provider": lookup_service_auth_provider,
                    "refresh_token": self.refresh_token,
                    "client_id": self.auth_client_id,
                }
            )
            log.debug(f"Original refresh_token: {self.refresh_token}")
            connection.request("PATCH", lookup_service_route, body, headers)
            response = connection.getresponse().read().decode()
            log.debug(f"response: {response}")
            userdata = json.loads(response)
            log.debug("User successfully reauthenticated.")
            self.bearer_token = userdata["access_token"]
            self.user = userdata["username"]
            self.refresh_token = userdata["refresh_token"]
            log.debug(f"refresh_token: {self.refresh_token}")
            self.jwt = _decode_bearer_token(self.bearer_token)


class RefreshableBotoSession:
    """
    Boto Helper class which lets us create refreshable session, so that we can cache the client or resource.

    Usage
    -----
    session = RefreshableBotoSession().refreshable_session()

    client = session.client("s3") # we now can cache this client object without worrying about expiring credentials
    """

    def __init__(self, session, session_ttl: int = 12 * 60 * 60):
        """
        Initialize `RefreshableBotoSession`

        Parameters
        ----------
        session : Session
            The session object to refresh

        session_ttl : int (optional)
            An integer number to set the TTL for each session. Beyond this session, it will renew the token.
        """

        self.session = session
        self.session_ttl = session_ttl

    def __get_session_credentials(self):
        """
        Get session credentials
        """
        sts_client = boto3.client(service_name="sts")
        try:
            sts_response = sts_client.assume_role_with_web_identity(
                RoleArn=self.session.sts_arn,
                RoleSessionName=self.session.user,
                WebIdentityToken=self.session.bearer_token,
                DurationSeconds=self.session_ttl,
            ).get("Credentials")
        except botocore.exceptions.ClientError as error:
            log.debug(f"Error code: {error.response['Error']['Code']}")
            if error.response["Error"]["Code"] == "ExpiredTokenException":
                log.debug("Bearer token has expired... Reauthenticating now")
                self.session.refresh_bearer_token()
                sts_response = sts_client.assume_role_with_web_identity(
                    RoleArn=self.session.sts_arn,
                    RoleSessionName=self.session.user,
                    WebIdentityToken=self.session.bearer_token,
                    DurationSeconds=self.session_ttl,
                ).get("Credentials")
            else:
                raise error
        # Token expire time logging
        bearer_expire_time = datetime.fromtimestamp(self.session.jwt["exp"]).strftime(
            "%H:%M:%S"
        )
        log.debug(f"Bearer token expire time: {bearer_expire_time}")
        if "sts_token" in self.session.__dict__:
            sts_expire_time = (
                self.session.sts_token._session.get_credentials()
                .__dict__["_expiry_time"]
                .replace(tzinfo=timezone.utc)
                .astimezone(tz=None)
                .strftime("%H:%M:%S")
            )
            log.debug(f"STS token expire time: {sts_expire_time}")

        credentials = {
            "access_key": sts_response.get("AccessKeyId"),
            "secret_key": sts_response.get("SecretAccessKey"),
            "token": sts_response.get("SessionToken"),
            "expiry_time": sts_response.get("Expiration").isoformat(),
        }

        return credentials

    def refreshable_session(self) -> boto3.Session:
        """
        Get refreshable boto3 session.
        """
        # get refreshable credentials
        refreshable_credentials = RefreshableCredentials.create_from_metadata(
            metadata=self.__get_session_credentials(),
            refresh_using=self.__get_session_credentials,
            method="sts-assume-role-with-web-identity",
        )

        # attach refreshable credentials current session
        session = get_session()
        session._credentials = refreshable_credentials
        autorefresh_session = boto3.Session(botocore_session=session)

        return autorefresh_session