# arkas/__init__.py
from flask import Flask
from .config import SECRET_KEY, ensure_folders
from .db_init import init_db
from .routes import bp as main_bp

def create_app():
    ensure_folders()
    init_db()

    app = Flask(__name__, template_folder="../templates", static_folder="../static")
    app.secret_key = SECRET_KEY

    app.register_blueprint(main_bp)
    return app