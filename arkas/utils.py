import os
import pandas as pd
from flask import request
from .config import ALLOWED_EXT, ALLOWED_PDF, ALLOWED_IMG

def allowed_file(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXT

def allowed_pdf(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_PDF

def allowed_img(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_IMG

def parse_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.astype(str).str.strip(), errors="coerce", dayfirst=True)

def get_paging_args(default_per_page=25):
    page_raw = request.values.get("page", "1")
    pp_raw = request.values.get("per_page", str(default_per_page))

    try:
        page = int(page_raw)
        if page < 1:
            page = 1
    except Exception:
        page = 1

    try:
        per_page = int(pp_raw)
        if per_page not in (25, 50, 100, 200):
            per_page = default_per_page
    except Exception:
        per_page = default_per_page

    return page, per_page