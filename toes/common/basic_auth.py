from base64 import b64encode
from functools import wraps

from flask import Response, request

from common.settings import settings


def _check_auth(username, password, setting_name):
    return username == getattr(
        settings, f"{setting_name}_username"
    ) and password == getattr(settings, f"{setting_name}_password")


def _authenticate():
    return Response(
        "Could not verify your access level for that URL. You have to login with proper credentials",
        401,
        {"WWW-Authenticate": 'Basic realm="Login Required"'},
    )


def requires_auth(auth_setting_name):
    def requires_auth_decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            auth = request.authorization
            if not auth or not _check_auth(
                auth.username, auth.password, auth_setting_name
            ):
                return _authenticate()
            return f(*args, **kwargs)

        return decorated

    return requires_auth_decorator


def mock_basic_auth(auth_setting_name):
    settings.override_cached_property(
        f"{auth_setting_name}_username", f"test_user_{auth_setting_name}"
    )
    settings.override_cached_property(
        f"{auth_setting_name}_password", f"test_password_{auth_setting_name}"
    )
    auth_string = f"test_user_{auth_setting_name}:test_password_{auth_setting_name}"
    auth_string_encoded = b64encode(bytes(auth_string, "utf-8")).decode("ascii")
    return f"Basic {auth_string_encoded}"


def mock_wrong_auth(auth_setting_name):
    settings.override_cached_property(
        f"{auth_setting_name}_username", f"test_user_{auth_setting_name}"
    )
    settings.override_cached_property(
        f"{auth_setting_name}_password", f"test_password_{auth_setting_name}"
    )
    return f'Basic {b64encode(b"wrong_user_{auth_setting_name}:wrong_password_{auth_setting_name}").decode("ascii")}'
