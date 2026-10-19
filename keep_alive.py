import os
import logging
from threading import Thread
from flask import Flask

app = Flask('')

@app.route('/')
def home():
    return "Bot is active and running on Render!"

def run_server():
    # Render assigns an HTTP port dynamically via the PORT environment variable
    port = int(os.environ.get("PORT", 10000))
    # Disable Flask's default debug logs to keep stdout clean
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_server)
    t.daemon = True
    t.start()