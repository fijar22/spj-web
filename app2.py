# app.py
from __future__ import annotations

from flask import Flask

from arkas.config import SECRET_KEY, ensure_folders
from arkas.db_init import init_db
from arkas.routes import bp as web_bp


def create_app() -> Flask:
    ensure_folders()
    init_db()

    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.secret_key = SECRET_KEY

    app.register_blueprint(web_bp)
    return app


app = create_app()

if __name__ == "__main__":
    # jalanin: python app.py
    app.run(debug=True)