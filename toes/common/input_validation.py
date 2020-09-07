from flask import abort, jsonify


def extract_personal_reason(t):
    t = t.strip()
    # TODO length validation
    return t


def make_error_response(code, message):
    res = jsonify({"error": {"code": code, "message": message}})
    res.status_code = code
    return res


def validate_payload(json_payload, *required_fields):
    """Helper to assert that a list of required keys are present in a JSON payload."""
    if not json_payload:
        abort(make_error_response(400, "Missing JSON payload"))

    missing_fields = [f for f in required_fields if json_payload.get(f) is None]
    if len(missing_fields) != 0:
        abort(
            make_error_response(
                400, f"Missing required fields: {', '.join(missing_fields)}"
            )
        )
