# arkas/db_init.py
from __future__ import annotations

from .db import get_conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # ======================================================
    # 1) CREATE TABLES (jika belum ada)
    # ======================================================

    # Master Kegiatan
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_kegiatan (
            kode_kegiatan TEXT,
            nama_kegiatan TEXT
        )
    """)

    # Master Rekening
    cur.execute("""
        CREATE TABLE IF NOT EXISTS master_rekening (
            kode_rekening_belanja TEXT,
            nama_rekening_belanja TEXT,
            rekap_rekening_belanja TEXT
        )
    """)

    # BKU
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bku (
            [Tgl] TEXT, [Keg] TEXT, [Rek] TEXT, [Bukti] TEXT,
            [Uraian] TEXT, [In] TEXT, [Out] TEXT, [Saldo] TEXT
        )
    """)

    # BHP / BHM
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bhp_bhm (
            [Tanggal] TEXT, [Kode Kegiatan] TEXT, [Kode Rekening] TEXT,
            [No Bukti] TEXT, [ID Barang] TEXT, [Uraian] TEXT,
            [Jumlah Barang] TEXT, [Harga Satuan] TEXT, [Realisasi] TEXT,
            [Sumber Data] TEXT
        )
    """)

    # Settings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            nama_sekolah TEXT, npsn TEXT, alamat TEXT, kab_kota TEXT, tahun TEXT,
            tempat_ttd TEXT, kepala_sekolah_nama TEXT, kepala_sekolah_nip TEXT,
            bendahara_nama TEXT, bendahara_nip TEXT,

            pihak1_nama TEXT, pihak1_jabatan TEXT, pihak1_perusahaan TEXT, pihak1_alamat TEXT, pihak1_telp TEXT,
            pihak2_nama TEXT, pihak2_jabatan TEXT, pihak2_nama_satdik TEXT, pihak2_alamat TEXT, pihak2_telp TEXT
        )
    """)
    cur.execute("INSERT OR IGNORE INTO app_settings (id) VALUES (1)")

    # BPU Override (tabel awal)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bpu_override (
            bpu TEXT PRIMARY KEY,
            kegiatan_override TEXT,
            created_at TEXT
        )
    """)

    # Foto BPU
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bpu_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bpu TEXT,
            filename TEXT,
            uploaded_at TEXT
        )
    """)

    # ======================================================
    # 1b) HISTORY PIHAK 1 (autocomplete)
    # ======================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pihak1_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nama TEXT NOT NULL,
            jabatan TEXT,
            perusahaan TEXT,
            alamat TEXT,
            telp TEXT,
            last_used_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pihak1_history_nama ON pihak1_history(nama)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pihak1_history_last_used ON pihak1_history(last_used_at)")

    # ======================================================
    # 2) MIGRASI KOLOM (untuk database versi lama)
    # ======================================================

    # ---- migrasi bpu_override: tambah kolom pihak1_*
    existing_cols = [r[1] for r in cur.execute("PRAGMA table_info(bpu_override)").fetchall()]

    needed_columns = [
        ("pihak1_nama", "TEXT"),
        ("pihak1_jabatan", "TEXT"),
        ("pihak1_perusahaan", "TEXT"),
        ("pihak1_alamat", "TEXT"),
        ("pihak1_telp", "TEXT"),
    ]

    for col_name, col_type in needed_columns:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE bpu_override ADD COLUMN {col_name} {col_type}")

    # (Opsional) index untuk cepat cari override per bpu (sebetulnya sudah PK)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bpu_override_bpu ON bpu_override(bpu)")

    # Foto: index bpu
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bpu_photos_bpu ON bpu_photos(bpu)")

    conn.commit()
    conn.close()