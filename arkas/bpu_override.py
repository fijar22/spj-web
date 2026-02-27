import os
from datetime import datetime
from werkzeug.utils import secure_filename
from .db import get_conn
from .config import STATIC_PHOTO_DIR

# arkas/bpu_override.py

def get_bpu_override(bpu: str) -> dict:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT
              bpu,
              kegiatan_override,
              pihak1_nama,
              pihak1_jabatan,
              pihak1_perusahaan,
              pihak1_alamat,
              pihak1_telp
            FROM bpu_override
            WHERE bpu=?
            """,
            (bpu,),
        ).fetchone()

        if not row:
            return {
                "bpu": bpu,
                "kegiatan_override": "",
                "pihak1_nama": "",
                "pihak1_jabatan": "",
                "pihak1_perusahaan": "",
                "pihak1_alamat": "",
                "pihak1_telp": "",
            }

        return {
            "bpu": row[0],
            "kegiatan_override": row[1] or "",
            "pihak1_nama": row[2] or "",
            "pihak1_jabatan": row[3] or "",
            "pihak1_perusahaan": row[4] or "",
            "pihak1_alamat": row[5] or "",
            "pihak1_telp": row[6] or "",
        }
    finally:
        conn.close()


def upsert_bpu_override(
    bpu: str,
    kegiatan: str,
    p1_nama: str,
    p1_jabatan: str,
    p1_perusahaan: str,
    p1_alamat: str,
    p1_telp: str,
):
    conn = get_conn()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """
            INSERT INTO bpu_override
              (bpu, kegiatan_override, pihak1_nama, pihak1_jabatan, pihak1_perusahaan, pihak1_alamat, pihak1_telp, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(bpu) DO UPDATE SET
              kegiatan_override=excluded.kegiatan_override,
              pihak1_nama=excluded.pihak1_nama,
              pihak1_jabatan=excluded.pihak1_jabatan,
              pihak1_perusahaan=excluded.pihak1_perusahaan,
              pihak1_alamat=excluded.pihak1_alamat,
              pihak1_telp=excluded.pihak1_telp
            """,
            (bpu, kegiatan, p1_nama, p1_jabatan, p1_perusahaan, p1_alamat, p1_telp, now),
        )
        conn.commit()
    finally:
        conn.close()

# --- Fungsi Pengelolaan Foto ---

def list_bpu_photos(bpu: str) -> list[dict]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, filename, uploaded_at FROM bpu_photos WHERE bpu=? ORDER BY id DESC",
            (bpu,),
        ).fetchall()
        out = []
        for rid, fn, ts in rows:
            out.append({
                "id": rid, 
                "filename": fn, 
                "uploaded_at": ts or "", 
                "url": f"/static/uploads/bpu_photos/{fn}"
            })
        return out
    finally:
        conn.close()

def add_bpu_photo(bpu: str, filename: str):
    conn = get_conn()
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("INSERT INTO bpu_photos (bpu, filename, uploaded_at) VALUES (?, ?, ?)", 
                     (bpu, filename, ts))
        conn.commit()
    finally:
        conn.close()

def save_uploaded_photo(bpu: str, file_storage) -> str:
    """Menyimpan file fisik ke folder statis."""
    base = secure_filename(file_storage.filename)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fn = f"{bpu}_{ts}_{base}"
    
    if not os.path.exists(STATIC_PHOTO_DIR):
        os.makedirs(STATIC_PHOTO_DIR, exist_ok=True)
        
    save_path = os.path.join(STATIC_PHOTO_DIR, fn)
    file_storage.save(save_path)
    return fn

def delete_bpu_photo(photo_id: int) -> bool:
    """Hapus row di DB dan file fisik terkait."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT filename FROM bpu_photos WHERE id=?",
            (int(photo_id),),
        ).fetchone()
        
        if not row:
            return False

        filename = row[0]
        conn.execute("DELETE FROM bpu_photos WHERE id=?", (int(photo_id),))
        conn.commit()
        
        # Hapus file fisik
        path = os.path.join(STATIC_PHOTO_DIR, filename)
        if os.path.exists(path):
            os.remove(path)
            
        return True
    finally:
        conn.close()

def delete_all_photos_for_bpu(bpu: str) -> int:
    """Hapus semua foto milik satu BPU."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT filename FROM bpu_photos WHERE bpu=?",
            (bpu,),
        ).fetchall()

        conn.execute("DELETE FROM bpu_photos WHERE bpu=?", (bpu,))
        conn.commit()
        
        deleted_count = 0
        for (fn,) in rows:
            try:
                path = os.path.join(STATIC_PHOTO_DIR, fn)
                if os.path.exists(path):
                    os.remove(path)
                deleted_count += 1
            except Exception:
                pass
        return deleted_count
    finally:
        conn.close()