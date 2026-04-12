import json

from flask import Flask, render_template

from .plugin_manager import get_plugin
from .standalone import build_page_data_script

app = Flask(__name__)


@app.route("/")
def index():
    plugin = get_plugin("core-event")
    payload = json.loads(plugin.read_asset_text("samples/dev-data.json"))
    page_data = plugin.build_page_data(payload)
    return render_template("index.html", page_data_script=build_page_data_script(page_data))


if __name__ == "__main__":
    app.run(debug=True, port=8080)
