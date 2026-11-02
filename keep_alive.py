import os
import logging
from flask import Flask

app = Flask('')

@app.route('/')
def home():
    return "Bot is active and running on Render!", 200

def keep_alive():
    port = int(os.environ.get("PORT", 10000))
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app.run(host='0.0.0.0', port=port)
