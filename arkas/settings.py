from .db import get_conn

def get_settings() -> dict:
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM app_settings WHERE id = 1").fetchone()
        cols = [x[1] for x in conn.execute("PRAGMA table_info(app_settings)").fetchall()]
        data = dict(zip(cols, row)) if row else {}
    finally:
        conn.close()

    defaults = {
        "nama_sekolah": "",
        "npsn": "",
        "alamat": "",
        "kab_kota": "",
        "tahun": "",
        "tempat_ttd": "",
        "kepala_sekolah_nama": "",
        "kepala_sekolah_nip": "",
        "bendahara_nama": "",
        "bendahara_nip": "",

        # BAST SIPLAH global default
        "pihak1_nama": "",
        "pihak1_jabatan": "",
        "pihak1_perusahaan": "",
        "pihak1_alamat": "",
        "pihak1_telp": "",
        "pihak2_nama": "",
        "pihak2_jabatan": "",
        "pihak2_nama_satdik": "",
        "pihak2_alamat": "",
        "pihak2_telp": "",
    }
    defaults.update({k: (data.get(k) or "") for k in defaults.keys()})
    return defaults


def save_settings(form: dict):
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE app_settings SET
                nama_sekolah = ?,
                npsn = ?,
                alamat = ?,
                kab_kota = ?,
                tahun = ?,
                tempat_ttd = ?,
                kepala_sekolah_nama = ?,
                kepala_sekolah_nip = ?,
                bendahara_nama = ?,
                bendahara_nip = ?,
                pihak1_nama = ?,
                pihak1_jabatan = ?,
                pihak1_perusahaan = ?,
                pihak1_alamat = ?,
                pihak1_telp = ?,
                pihak2_nama = ?,
                pihak2_jabatan = ?,
                pihak2_nama_satdik = ?,
                pihak2_alamat = ?,
                pihak2_telp = ?
            WHERE id = 1
            """,
            (
                (form.get("nama_sekolah") or "").strip(),
                (form.get("npsn") or "").strip(),
                (form.get("alamat") or "").strip(),
                (form.get("kab_kota") or "").strip(),
                (form.get("tahun") or "").strip(),
                (form.get("tempat_ttd") or "").strip(),
                (form.get("kepala_sekolah_nama") or "").strip(),
                (form.get("kepala_sekolah_nip") or "").strip(),
                (form.get("bendahara_nama") or "").strip(),
                (form.get("bendahara_nip") or "").strip(),
                (form.get("pihak1_nama") or "").strip(),
                (form.get("pihak1_jabatan") or "").strip(),
                (form.get("pihak1_perusahaan") or "").strip(),
                (form.get("pihak1_alamat") or "").strip(),
                (form.get("pihak1_telp") or "").strip(),
                (form.get("pihak2_nama") or "").strip(),
                (form.get("pihak2_jabatan") or "").strip(),
                (form.get("pihak2_nama_satdik") or "").strip(),
                (form.get("pihak2_alamat") or "").strip(),
                (form.get("pihak2_telp") or "").strip(),
            ),
        )
        conn.commit()
    finally:
        conn.close()