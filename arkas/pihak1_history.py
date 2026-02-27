# arkas/pihak1_history.py
from __future__ import annotations
from datetime import datetime
from .db import get_conn

def upsert_history_pihak1(nama: str, jabatan: str, perusahaan: str, alamat: str, telp: str):
    nama = (nama or "").strip()
    if not nama:
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    try:
        # cari existing by nama (case-insensitive)
        row = conn.execute(
            "SELECT id FROM pihak1_history WHERE lower(nama)=lower(?)",
            (nama,),
        ).fetchone()

        if row:
            conn.execute(
                """
                UPDATE pihak1_history
                SET jabatan=?, perusahaan=?, alamat=?, telp=?, last_used_at=?
                WHERE id=?
                """,
                ((jabatan or "").strip(), (perusahaan or "").strip(), (alamat or "").strip(), (telp or "").strip(), now, row[0]),
            )
        else:
            conn.execute(
                """
                INSERT INTO pihak1_history (nama, jabatan, perusahaan, alamat, telp, last_used_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (nama, (jabatan or "").strip(), (perusahaan or "").strip(), (alamat or "").strip(), (telp or "").strip(), now),
            )
        conn.commit()
    finally:
        conn.close()

def search_history_pihak1(q: str, limit: int = 10) -> list[dict]:
    q = (q or "").strip()
    if len(q) < 3:
        return []

    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT nama, jabatan, perusahaan, alamat, telp
            FROM pihak1_history
            WHERE lower(nama) LIKE lower(?)
            ORDER BY last_used_at DESC, nama ASC
            LIMIT ?
            """,
            (f"%{q}%", int(limit)),
        ).fetchall()

        return [
            {"nama": r[0] or "", "jabatan": r[1] or "", "perusahaan": r[2] or "", "alamat": r[3] or "", "telp": r[4] or ""}
            for r in rows
        ]
    finally:
        conn.close()

def get_history_by_nama(nama: str) -> dict | None:
    nama = (nama or "").strip()
    if not nama:
        return None

    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT nama, jabatan, perusahaan, alamat, telp
            FROM pihak1_history
            WHERE lower(nama)=lower(?)
            LIMIT 1
            """,
            (nama,),
        ).fetchone()
        if not row:
            return None
        return {"nama": row[0] or "", "jabatan": row[1] or "", "perusahaan": row[2] or "", "alamat": row[3] or "", "telp": row[4] or ""}
    finally:
        conn.close()