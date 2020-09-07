from flask import request


def request_url():
    if request.headers.get("X-Forwarded-Proto") == "https":
        return request.url.replace("http://", "https://")
    else:
        return request.url
