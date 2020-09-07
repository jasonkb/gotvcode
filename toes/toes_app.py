from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from actblue.actblue import mod as actblue_module
from bling.api import mod as bling_module
from caucus_app.api import mod as caucus_module
from contentful_webhook.api import mod as contentful_module
from donors.donors import mod as donors_module
from generic_kv.generic_kv import mod as generic_kv_module
from help_scout_contact_us.api import mod as help_scout_contact_us
from mdata.mdata import mod as mdata_module
from mobilize_america.mobilize_america import mod as mobilize_america_module
from mobilizeio.webhook_handler import mod as mobilizeio_module

app = Flask(__name__)
CORS(app)
app.register_blueprint(actblue_module, url_prefix="/actblue")
app.register_blueprint(generic_kv_module, url_prefix="/generic_kv")
app.register_blueprint(mdata_module, url_prefix="/mdata")
app.register_blueprint(mobilize_america_module, url_prefix="/mobilize_america")
app.register_blueprint(donors_module, url_prefix="/donors")


@app.route("/")
def index():
    return "Let's GOTV!", 200


@app.route("/error")
def error():
    raise Exception("TEST TEST")


if __name__ == "__main__":
    app.run()
