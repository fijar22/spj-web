import sqlite3
from .config import DB_PATH

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # master
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_kegiatan (
            kode_kegiatan TEXT,
            nama_kegiatan TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_rekening (
            kode_rekening_belanja TEXT,
            nama_rekening_belanja TEXT,
            rekap_rekening_belanja TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_master_kegiatan_kode ON master_kegiatan(kode_kegiatan)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_master_rekening_kode ON master_rekening(kode_rekening_belanja)")

    # settings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            nama_sekolah TEXT,
            npsn TEXT,
            alamat TEXT,
            kab_kota TEXT,
            tahun TEXT,
            tempat_ttd TEXT,
            kepala_sekolah_nama TEXT,
            kepala_sekolah_nip TEXT,
            bendahara_nama TEXT,
            bendahara_nip TEXT
        )
    """)
    cur.execute("INSERT OR IGNORE INTO app_settings (id) VALUES (1)")

    # tambah kolom BAST default global
    cols = [r[1] for r in cur.execute("PRAGMA table_info(app_settings)").fetchall()]
    add_cols = [
        ("pihak1_nama", "TEXT"),
        ("pihak1_jabatan", "TEXT"),
        ("pihak1_perusahaan", "TEXT"),
        ("pihak1_alamat", "TEXT"),
        ("pihak1_telp", "TEXT"),
        ("pihak2_nama", "TEXT"),
        ("pihak2_jabatan", "TEXT"),
        ("pihak2_nama_satdik", "TEXT"),
        ("pihak2_alamat", "TEXT"),
        ("pihak2_telp", "TEXT"),
    ]
    for c, typ in add_cols:
        if c not in cols:
            cur.execute(f"ALTER TABLE app_settings ADD COLUMN {c} {typ}")

    # override per BPU
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bpu_override (
            bpu TEXT PRIMARY KEY,
            kegiatan_override TEXT,
            pihak1_nama TEXT,
            pihak1_jabatan TEXT,
            pihak1_perusahaan TEXT,
            created_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bpu_override_bpu ON bpu_override(bpu)")

    # foto per BPU
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bpu_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bpu TEXT,
            filename TEXT,
            uploaded_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bpu_photos_bpu ON bpu_photos(bpu)")

    conn.commit()
    conn.close()