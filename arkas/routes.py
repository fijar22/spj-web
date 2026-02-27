# arkas/routes.py
from __future__ import annotations

from io import BytesIO
import os

from flask import jsonify
from .pihak1_history import search_history_pihak1, upsert_history_pihak1

import pandas as pd
from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    send_file,
    abort,
)
from werkzeug.utils import secure_filename

from .config import (
    UPLOAD_FOLDER,
    PDF_UPLOAD_FOLDER,
    ALLOWED_EXT,
    ALLOWED_PDF,
    ALLOWED_IMG,
)
from .db import get_conn
from .settings import get_settings, save_settings

from .queries import (
    get_filter_options,
    get_bulan_options,
    get_paging_args,
    ambil_data_bku,
    ambil_data_bhp,
    ambil_spj_per_bpu,
    get_bpu_bku_rows,
    get_bpu_bhp_detail,
)

from .converters import convert_bku_pdfs, convert_bhp_pdfs
from .pdf_docs import buat_pdf_bast, buat_pdf_kwitansi
from .bpu_override import (
    get_bpu_override,
    upsert_bpu_override,
    save_uploaded_photo,
    add_bpu_photo,
    list_bpu_photos,
    delete_bpu_photo,
)

bp = Blueprint("main", __name__)


# =========================================================
# UTIL
# =========================================================
def allowed_file(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXT


def allowed_pdf(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_PDF


def allowed_img(filename: str) -> bool:
    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_IMG


def _prefill_kegiatan_from_bku(df_bku: pd.DataFrame) -> str:
    """
    Ambil kegiatan default dari BKU:
    - pakai kolom NamaKegiatan jika ada
    - fallback kolom Keg
    """
    if df_bku is None or df_bku.empty:
        return ""
    try:
        v = str(df_bku.get("NamaKegiatan", pd.Series([""])).iloc[0] or "").strip()
        if v:
            return v
    except Exception:
        pass
    try:
        v = str(df_bku.get("Keg", pd.Series([""])).iloc[0] or "").strip()
        return v
    except Exception:
        return ""


# =========================================================
# ROUTES: BKU / BHP / SPJ per BPU
# =========================================================
@bp.route("/", methods=["GET"])
def page_bku():
    kegiatan_list, rekap_list = get_filter_options()
    page, per_page = get_paging_args()

    filters = {
        "keyword": request.values.get("keyword", "").strip(),
        "kegiatan": request.values.get("kegiatan", "__ALL__"),
        "rekap": request.values.get("rekap", "__ALL__"),
        "tgl_from": request.values.get("tgl_from", "").strip(),
        "tgl_to": request.values.get("tgl_to", "").strip(),
    }

    df, summary, pagination = ambil_data_bku(filters, page, per_page)
    return render_template(
        "bku.html",
        data=df.to_dict(orient="records"),
        filters=filters,
        kegiatan_list=kegiatan_list,
        rekap_list=rekap_list,
        summary=summary,
        pagination=pagination,
    )


@bp.route("/bhp", methods=["GET"])
def page_bhp():
    kegiatan_list, rekap_list = get_filter_options()
    page, per_page = get_paging_args()

    filters = {
        "keyword": request.values.get("keyword", "").strip(),
        "kegiatan": request.values.get("kegiatan", "__ALL__"),
        "rekap": request.values.get("rekap", "__ALL__"),
        "tgl_from": request.values.get("tgl_from", "").strip(),
        "tgl_to": request.values.get("tgl_to", "").strip(),
    }

    df, summary, pagination = ambil_data_bhp(filters, page, per_page)
    return render_template(
        "bhp.html",
        data=df.to_dict(orient="records"),
        filters=filters,
        kegiatan_list=kegiatan_list,
        rekap_list=rekap_list,
        summary=summary,
        pagination=pagination,
    )


@bp.route("/spj-bpu", methods=["GET"])
def page_spj_bpu():
    kegiatan_list, rekap_list = get_filter_options()
    bulan_list = get_bulan_options()
    import arkas.queries as q
    print("USING QUERIES:", q.__file__)
    print("BULAN LIST:", bulan_list[:10], "TOTAL:", len(bulan_list))
    page, per_page = get_paging_args()

    filters = {
        "keyword": request.values.get("keyword", "").strip(),
        "kegiatan": request.values.get("kegiatan", "__ALL__"),
        "rekap": request.values.get("rekap", "__ALL__"),
        "bulan": request.values.get("bulan", "__ALL__"),
        "tgl_from": request.values.get("tgl_from", "").strip(),
        "tgl_to": request.values.get("tgl_to", "").strip(),
    }

    df, summary, pagination = ambil_spj_per_bpu(filters, page, per_page)
    return render_template(
        "spj_bpu.html",
        data=df.to_dict(orient="records"),
        filters=filters,
        kegiatan_list=kegiatan_list,
        rekap_list=rekap_list,
        summary=summary,
        pagination=pagination,
    )


# =========================================================
# EDIT BPU (Override kegiatan + pihak1 per BPU + Upload foto)
# =========================================================
@bp.route("/bpu/<bpu>/edit", methods=["GET", "POST"])
def page_edit_bpu(bpu: str):
    if request.method == "POST":
        kegiatan_override = (request.form.get("kegiatan_override") or "").strip()
        p1_nama = (request.form.get("pihak1_nama") or "").strip()
        p1_jabatan = (request.form.get("pihak1_jabatan") or "").strip()
        p1_perusahaan = (request.form.get("pihak1_perusahaan") or "").strip()
        p1_alamat = (request.form.get("pihak1_alamat") or "").strip()
        p1_telp = (request.form.get("pihak1_telp") or "").strip()

        upsert_bpu_override(
            bpu,
            kegiatan_override,
            p1_nama,
            p1_jabatan,
            p1_perusahaan,
            p1_alamat,
            p1_telp,
        )

        # simpan history pihak 1 (untuk autocomplete)
        upsert_history_pihak1(
            p1_nama,
            p1_jabatan,
            p1_perusahaan,
            p1_alamat,
            p1_telp,
        )

        # upload foto (opsional)
        f = request.files.get("photo")
        if f and f.filename:
            if not allowed_img(f.filename):
                flash("Format foto harus .jpg/.jpeg/.png/.webp", "error")
                return redirect(url_for("main.page_edit_bpu", bpu=bpu))
            fn = save_uploaded_photo(bpu, f)
            add_bpu_photo(bpu, fn)

        flash("✔ Data BPU tersimpan.", "ok")
        return redirect(url_for("main.page_edit_bpu", bpu=bpu))

    # GET
    ov = get_bpu_override(bpu)
    photos = list_bpu_photos(bpu)

    df_bku = get_bpu_bku_rows(bpu)
    default_kegiatan = _prefill_kegiatan_from_bku(df_bku)

    # Prefill: kalau override kosong, isi kegiatan BKU
    if (ov.get("kegiatan_override") or "").strip() == "" and default_kegiatan:
        ov["kegiatan_override"] = default_kegiatan

    return render_template(
        "edit_bpu.html",
        bpu=bpu,
        override=ov,
        photos=photos,
        default_kegiatan=default_kegiatan,
    )


# =========================================================
# DELETE FOTO per BPU
# (dipakai oleh form: /bast/<bpu>/photos/<id>/delete)
# =========================================================
@bp.route("/bast/<bpu>/photos/<int:photo_id>/delete", methods=["POST"])
def delete_bpu_photo_route(bpu: str, photo_id: int):
    ok = delete_bpu_photo(photo_id)
    if ok:
        flash("✔ Foto berhasil dihapus.", "ok")
    else:
        flash("Foto tidak ditemukan.", "error")
    return redirect(url_for("main.page_bast_detail", bpu=bpu))


# =========================================================
# BAST: DETAIL PAGE
# =========================================================
@bp.route("/bast/<bpu>", methods=["GET"])
def page_bast_detail(bpu: str):
    df_rows = get_bpu_bku_rows(bpu)
    df_detail = get_bpu_bhp_detail(bpu)

    if df_rows.empty:
        abort(404, f"BPU {bpu} tidak ditemukan")

    header = df_rows.iloc[0].to_dict()
    total_out = float(pd.to_numeric(df_rows["Out"], errors="coerce").fillna(0).sum())

    rows = df_rows.copy()
    rows["Out"] = pd.to_numeric(rows["Out"], errors="coerce").fillna(0).astype(float)
    rows["Out"] = rows["Out"].apply(lambda v: "Rp {:,.0f}".format(v).replace(",", "."))

    ov = get_bpu_override(bpu)
    photos = list_bpu_photos(bpu)

    # Prefill kegiatan override (kalau kosong) supaya di BAST juga ikut kebaca
    default_kegiatan = _prefill_kegiatan_from_bku(df_rows)
    if (ov.get("kegiatan_override") or "").strip() == "" and default_kegiatan:
        ov["kegiatan_override"] = default_kegiatan

    return render_template(
        "bast.html",
        bpu=bpu,
        header=header,
        total_out=total_out,
        rows=rows.to_dict(orient="records"),
        detail=df_detail.to_dict(orient="records") if (df_detail is not None and not df_detail.empty) else [],
        override=ov,
        photos=photos,
    )


@bp.route("/bast/<bpu>/pdf", methods=["GET"])
def download_bast_pdf(bpu: str):
    df_bku = get_bpu_bku_rows(bpu)
    df_detail = get_bpu_bhp_detail(bpu)

    if df_bku.empty:
        abort(404, f"BPU {bpu} tidak ditemukan")

    pdf_bytes = buat_pdf_bast(bpu, df_bku, df_detail)
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"BAST_{bpu}.pdf",
    )


# =========================================================
# BKP PDF (KWITANSI)
# =========================================================
@bp.route("/bkp/<bpu>/pdf", methods=["GET"])
def download_bkp_pdf(bpu: str):
    df = get_bpu_bku_rows(bpu)
    if df.empty:
        abort(404, f"BPU {bpu} tidak ditemukan")

    total = float(pd.to_numeric(df["Out"], errors="coerce").fillna(0).sum())
    tgl = str(df["Tgl"].iloc[0] or "").strip()

    st = get_settings()
    nama_sekolah = (st.get("nama_sekolah") or "").strip()

    # kegiatan bisa dioverride per bpu
    ov = get_bpu_override(bpu)
    nama_kegiatan = (ov.get("kegiatan_override") or "").strip()
    if not nama_kegiatan:
        nama_kegiatan = _prefill_kegiatan_from_bku(df)

    pdf_bytes = buat_pdf_kwitansi(
        bpu,
        {
            "nomor": bpu,
            "tgl": tgl,
            "telah_terima_dari": f"Bendahara BOSP {nama_sekolah}".strip(),
            "untuk_pembayaran": nama_kegiatan or "—",
            "jumlah": total,
        },
    )
    return send_file(
        BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"BKP_{bpu}.pdf",
    )


# =========================================================
# SETTINGS
# =========================================================
@bp.route("/settings", methods=["GET", "POST"])
def page_settings():
    if request.method == "POST":
        save_settings(request.form)
        flash("✔ Settings berhasil disimpan.", "ok")
        return redirect(url_for("main.page_settings"))

    return render_template("settings.html", settings=get_settings())


# =========================================================
# IMPORT MENU + IMPORT OUTPUT EXCEL + MASTER
# =========================================================
@bp.route("/import", methods=["GET"])
def import_menu():
    return render_template("import_menu.html")


@bp.route("/api/pihak1/search")
def api_pihak1_search():
    q = request.args.get("q", "").strip()
    items = search_history_pihak1(q, limit=10)
    return jsonify(items)


@bp.route("/import/output", methods=["GET", "POST"])
def import_output_excel():
    if request.method == "POST":
        mode = request.form.get("mode", "append")
        file = request.files.get("file")

        if not file or file.filename.strip() == "":
            flash("File belum dipilih.", "error")
            return redirect(url_for("main.import_output_excel"))

        if not allowed_file(file.filename):
            flash("File harus .xlsx", "error")
            return redirect(url_for("main.import_output_excel"))

        filename = secure_filename(file.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(save_path)

        conn = get_conn()
        try:
            try:
                df_bku = pd.read_excel(save_path, sheet_name="BKU")
                df_bku.to_sql("bku", conn, if_exists=mode, index=False)
                flash("✔ BKU berhasil diimport.", "ok")
            except Exception as e:
                flash(f"Sheet BKU tidak ditemukan / gagal dibaca: {e}", "error")

            try:
                df_bhp = pd.read_excel(save_path, sheet_name="BHP_BHM")
                df_bhp.to_sql("bhp_bhm", conn, if_exists=mode, index=False)
                flash("✔ BHP_BHM berhasil diimport.", "ok")
            except Exception as e:
                flash(f"Sheet BHP_BHM tidak ditemukan / gagal dibaca: {e}", "error")
        finally:
            conn.close()

        return redirect(url_for("main.import_output_excel"))

    return render_template("import_output.html")


@bp.route("/import/master/kegiatan", methods=["GET", "POST"])
def import_master_kegiatan():
    if request.method == "POST":
        file = request.files.get("file")

        if not file or file.filename.strip() == "":
            flash("File master kegiatan belum dipilih.", "error")
            return redirect(url_for("main.import_master_kegiatan"))

        if not allowed_file(file.filename):
            flash("File harus .xlsx", "error")
            return redirect(url_for("main.import_master_kegiatan"))

        filename = secure_filename(file.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(save_path)

        conn = get_conn()
        try:
            df = pd.read_excel(save_path)
            needed = {"kode_kegiatan", "nama_kegiatan"}
            if not needed.issubset(set(df.columns.astype(str))):
                flash(f"Kolom tidak sesuai. Kolom yang ada: {list(df.columns)}", "error")
                return redirect(url_for("main.import_master_kegiatan"))

            df = df[list(needed)].copy()
            df["kode_kegiatan"] = df["kode_kegiatan"].astype(str).str.strip()
            df["nama_kegiatan"] = df["nama_kegiatan"].astype(str).str.strip()

            df.to_sql("master_kegiatan", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_master_kegiatan_kode ON master_kegiatan(kode_kegiatan)")
            flash("✔ Master Kegiatan berhasil diimport (replace).", "ok")
        except Exception as e:
            flash(f"Gagal import master kegiatan: {e}", "error")
        finally:
            conn.close()

        return redirect(url_for("main.import_master_kegiatan"))

    return render_template("import_master_kegiatan.html")


@bp.route("/import/master/rekening", methods=["GET", "POST"])
def import_master_rekening():
    if request.method == "POST":
        file = request.files.get("file")

        if not file or file.filename.strip() == "":
            flash("File master rekening belum dipilih.", "error")
            return redirect(url_for("main.import_master_rekening"))

        if not allowed_file(file.filename):
            flash("File harus .xlsx", "error")
            return redirect(url_for("main.import_master_rekening"))

        filename = secure_filename(file.filename)
        save_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(save_path)

        conn = get_conn()
        try:
            df = pd.read_excel(save_path)
            needed = {"kode_rekening_belanja", "nama_rekening_belanja", "rekap_rekening_belanja"}
            if not needed.issubset(set(df.columns.astype(str))):
                flash(f"Kolom tidak sesuai. Kolom yang ada: {list(df.columns)}", "error")
                return redirect(url_for("main.import_master_rekening"))

            df = df[list(needed)].copy()
            df["kode_rekening_belanja"] = df["kode_rekening_belanja"].astype(str).str.strip()
            df["nama_rekening_belanja"] = df["nama_rekening_belanja"].astype(str).str.strip()
            df["rekap_rekening_belanja"] = df["rekap_rekening_belanja"].astype(str).str.strip()

            df.to_sql("master_rekening", conn, if_exists="replace", index=False)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_master_rekening_kode ON master_rekening(kode_rekening_belanja)")
            flash("✔ Master Rekening berhasil diimport (replace).", "ok")
        except Exception as e:
            flash(f"Gagal import master rekening: {e}", "error")
        finally:
            conn.close()

        return redirect(url_for("main.import_master_rekening"))

    return render_template("import_master_rekening.html")


@bp.route("/import/reset-data", methods=["POST"])
def reset_data():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM bku")
        cur.execute("DELETE FROM bhp_bhm")
        conn.commit()
    except Exception as e:
        flash(f"Gagal reset data: {e}", "error")
        return redirect(url_for("main.import_menu"))
    finally:
        conn.close()

    flash("✔ Semua data BKU dan BHP/BHM berhasil direset.", "ok")
    return redirect(url_for("main.import_menu"))


# =========================================================
# CONVERT PDF -> EXCEL + (optional) import to DB
# =========================================================
@bp.route("/convert", methods=["GET"])
def page_convert():
    return render_template("convert.html")


@bp.route("/convert/run", methods=["POST"])
def convert_run():
    mode = request.form.get("mode", "both")  # bku / bhp / both
    import_now = request.form.get("import_now") == "1"
    db_mode = request.form.get("db_mode", "append")  # append / replace

    bku_files = request.files.getlist("bku_pdfs")
    bhp_files = request.files.getlist("bhp_pdfs")

    saved_bku: list[str] = []
    saved_bhp: list[str] = []

    if mode in ("bku", "both"):
        for f in bku_files:
            if f and f.filename and allowed_pdf(f.filename):
                name = secure_filename(f.filename)
                path = os.path.join(PDF_UPLOAD_FOLDER, name)
                f.save(path)
                saved_bku.append(path)

    if mode in ("bhp", "both"):
        for f in bhp_files:
            if f and f.filename and allowed_pdf(f.filename):
                name = secure_filename(f.filename)
                path = os.path.join(PDF_UPLOAD_FOLDER, name)
                f.save(path)
                saved_bhp.append(path)

    if mode in ("bku", "both") and not saved_bku:
        flash("PDF BKU belum dipilih.", "error")
        return redirect(url_for("main.page_convert"))

    if mode in ("bhp", "both") and not saved_bhp:
        flash("PDF BHP/BHM belum dipilih.", "error")
        return redirect(url_for("main.page_convert"))

    try:
        df_bku = convert_bku_pdfs(saved_bku) if mode in ("bku", "both") else None
        df_bhp = convert_bhp_pdfs(saved_bhp) if mode in ("bhp", "both") else None
    except Exception as e:
        flash(f"Gagal convert: {e}", "error")
        return redirect(url_for("main.page_convert"))

    if import_now:
        conn = get_conn()
        try:
            if df_bku is not None:
                df_bku.to_sql("bku", conn, if_exists=db_mode, index=False)
                flash("✔ BKU hasil convert berhasil diimport ke database.", "ok")

            if df_bhp is not None:
                df_bhp.to_sql("bhp_bhm", conn, if_exists=db_mode, index=False)
                flash("✔ BHP_BHM hasil convert berhasil diimport ke database.", "ok")

        except Exception as e:
            flash(f"Gagal import ke database: {e}", "error")
            return redirect(url_for("main.page_convert"))
        finally:
            conn.close()

        if mode == "bku":
            return redirect(url_for("main.page_bku"))
        if mode == "bhp":
            return redirect(url_for("main.page_bhp"))
        return redirect(url_for("main.page_spj_bpu"))

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        if df_bku is not None:
            df_bku.to_excel(writer, sheet_name="BKU", index=False)
        if df_bhp is not None:
            df_bhp.to_excel(writer, sheet_name="BHP_BHM", index=False)
    out.seek(0)

    return send_file(
        out,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="OUTPUT_ARKAS.xlsx",
    )