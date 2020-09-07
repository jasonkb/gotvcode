from flask import Blueprint, Response, request

from common.basic_auth import requires_auth
from models.generic_kv import GenericKV

mod = Blueprint("generic_kv", __name__)


@mod.route("/value", methods=["GET"])
@requires_auth("generic_kv_read")
def value_get():
    k = request.args.get("k", "").strip()
    if not k:
        return ("Generic KV requires a key", 400)
    try:
        generic_kv = GenericKV.get(k)
    except GenericKV.DoesNotExist:
        return (f"Key {k} not found", 404)
    return Response(response=generic_kv.v, mimetype="application/json")


@mod.route("/value", methods=["POST", "PUT"])
@requires_auth("generic_kv_write")
def value_put():
    k = request.args.get("k", "").strip()
    v = request.data.decode("utf-8")
    if not k:
        return ("Generic KV requires a key", 400)
    GenericKV.put_value(k, v)
    return ("", 204)
