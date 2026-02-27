import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Jika config.py ada di folder arkas/, maka:
# BASE_DIR = folder project utama (yang ada app.py)

SECRET_KEY = "arkas-secret-key"

DB_PATH = os.path.join(BASE_DIR, "arkas.db")

UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
PDF_UPLOAD_FOLDER = os.path.join(BASE_DIR, "pdf_uploads")

STATIC_DIR = os.path.join(BASE_DIR, "static")
STATIC_PHOTO_DIR = os.path.join(STATIC_DIR, "uploads", "bpu_photos")

ALLOWED_EXT = {".xlsx"}
ALLOWED_PDF = {".pdf"}
ALLOWED_IMG = {".jpg", ".jpeg", ".png", ".webp"}

def ensure_folders():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PDF_UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(STATIC_PHOTO_DIR, exist_ok=True)