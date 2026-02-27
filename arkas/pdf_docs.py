from __future__ import annotations

import os
import pandas as pd
# Gunakan Pillow (PIL) sebagai pendukung ReportLab untuk pemrosesan gambar
from io import BytesIO

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer,
    Image, PageBreak
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfgen import canvas

from .config import STATIC_PHOTO_DIR
from .settings import get_settings
from .bpu_override import get_bpu_override, list_bpu_photos


# =========================================================
# UTIL: TERBILANG
# =========================================================
def terbilang(n: int) -> str:
    n = int(n)
    satuan = ["", "SATU", "DUA", "TIGA", "EMPAT", "LIMA", "ENAM", "TUJUH", "DELAPAN", "SEMBILAN", "SEPULUH", "SEBELAS"]

    def _t(x: int) -> str:
        x = int(x)
        if x < 12:
            return satuan[x]
        if x < 20:
            return _t(x - 10) + " BELAS"
        if x < 100:
            puluh = x // 10
            sisa = x % 10
            return (satuan[puluh] + " PULUH " + _t(sisa)).strip()
        if x < 200:
            return ("SERATUS " + _t(x - 100)).strip()
        if x < 1000:
            ratus = x // 100
            sisa = x % 100
            return (satuan[ratus] + " RATUS " + _t(sisa)).strip()
        if x < 2000:
            return ("SERIBU " + _t(x - 1000)).strip()
        if x < 1_000_000:
            ribu = x // 1000
            sisa = x % 1000
            return (_t(ribu) + " RIBU " + _t(sisa)).strip()
        if x < 1_000_000_000:
            juta = x // 1_000_000
            sisa = x % 1_000_000
            return (_t(juta) + " JUTA " + _t(sisa)).strip()
        m = x // 1_000_000_000
        s = x % 1_000_000_000
        return (_t(m) + " MILIAR " + _t(s)).strip()

    out = _t(n).strip()
    return out if out else "NOL"


# =========================================================
# BKP / KWITANSI (LANDSCAPE)
# =========================================================
def buat_pdf_kwitansi(bpu: str, data: dict) -> bytes:
    settings = get_settings()

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=landscape(A4))
    w, h = landscape(A4)

    left = 2.0 * cm
    right = w - 2.0 * cm
    top = h - 2.0 * cm
    bottom = 2.0 * cm

    # Border
    c.setLineWidth(1.2)
    c.rect(left, bottom, right - left, top - bottom)
    c.setLineWidth(0.6)
    c.rect(left + 0.25 * cm, bottom + 0.25 * cm, (right - left) - 0.5 * cm, (top - bottom) - 0.5 * cm)

    # Header
    header_h = 1.3 * cm
    header_w = 9 * cm
    hx = (w - header_w) / 2
    hy = top - 1.6 * cm

    c.setFillColor(colors.whitesmoke)
    c.rect(hx, hy, header_w, header_h, stroke=1, fill=1)
    c.setFillColor(colors.black)

    c.setFont("Times-Bold", 16)
    c.drawCentredString(w / 2, hy + 0.42 * cm, "BUKTI KAS PENGELUARAN")

    label_w = 4.2 * cm
    value_x = left + label_w + 0.2 * cm
    line_gap = 0.95 * cm

    nomor = data.get("nomor") or bpu
    nomor_y = hy - 0.6 * cm

    c.setFont("Times-Roman", 11)
    c.drawString(left + 0.6 * cm, nomor_y, "Nomor")
    c.drawString(left + label_w - 0.3 * cm, nomor_y, ":")
    c.setFont("Times-Bold", 11)
    c.drawString(value_x, nomor_y, str(nomor))

    y = hy - 1.7 * cm

    def field(label: str, value: str, yy: float, underline=True):
        c.setFont("Times-Roman", 11)
        c.drawString(left + 0.6 * cm, yy, label)
        c.drawString(left + label_w - 0.3 * cm, yy, ":")
        c.setFont("Times-Bold", 11)
        c.drawString(value_x, yy, value)
        if underline:
            c.line(value_x, yy - 3, right - 0.6 * cm, yy - 3)

    def draw_wrapped(value: str, x: float, yy: float, max_width: float):
        value = (value or "—").strip()
        c.setFont("Times-Bold", 11)

        words = value.split()
        lines = []
        cur = ""

        for wds in words:
            test = (cur + " " + wds).strip()
            if c.stringWidth(test, "Times-Bold", 11) <= max_width:
                cur = test
            else:
                if cur:
                    lines.append(cur)
                cur = wds
        if cur:
            lines.append(cur)

        lines = lines[:3]
        ycur = yy
        for li in lines:
            c.drawString(x, ycur, li)
            c.line(x, ycur - 3, x + max_width, ycur - 3)
            ycur -= 0.65 * cm
        return ycur

    field("Telah terima dari", data.get("telah_terima_dari") or "—", y)
    y -= line_gap

    jumlah = float(data.get("jumlah") or 0)
    ter = f"{terbilang(int(round(jumlah)))} RUPIAH"

    c.setFont("Times-Roman", 11)
    c.drawString(left + 0.6 * cm, y, "Uang sejumlah")
    c.drawString(left + label_w - 0.3 * cm, y, ":")
    c.setFont("Times-Bold", 11)

    max_width = right - value_x - 0.6 * cm
    y = draw_wrapped(ter, value_x, y, max_width)
    y -= 0.2 * cm

    c.setFont("Times-Roman", 11)
    c.drawString(left + 0.6 * cm, y, "Untuk pembayaran")
    c.drawString(left + label_w - 0.3 * cm, y, ":")
    y = draw_wrapped(data.get("untuk_pembayaran") or "—", value_x, y, max_width)
    y -= 0.3 * cm

    box_w = 6.0 * cm
    box_h = 1.4 * cm
    bx = value_x
    by = y - 2 * cm

    c.setFillColor(colors.whitesmoke)
    c.rect(bx, by, box_w, box_h, stroke=1, fill=1)
    c.setFillColor(colors.black)

    c.setFont("Times-Bold", 14)
    c.drawString(bx + 0.6 * cm, by + 0.42 * cm, "Rp.")
    c.drawRightString(bx + box_w - 0.6 * cm, by + 0.42 * cm, "{:,.0f}".format(jumlah).replace(",", "."))

    y_ttd = bottom + 4.2 * cm

    tempat = (settings.get("tempat_ttd") or settings.get("kab_kota") or "—").strip()
    tanggal_fix = data.get("tgl") or ""

    col_w = (right - left - 1.2 * cm) / 3.0
    x1 = left + 0.6 * cm
    x2 = x1 + col_w
    x3 = x2 + col_w

    c.setFont("Times-Roman", 11)
    c.drawString(x3, y_ttd + 2.6 * cm, f"{tempat}, {tanggal_fix}".strip())

    c.drawString(x1, y_ttd + 1.8 * cm, "Mengetahui,")
    c.drawString(x1, y_ttd + 1.2 * cm, "Kepala Sekolah")
    c.drawString(x2, y_ttd + 1.2 * cm, "Bendahara")
    c.drawString(x3, y_ttd + 1.2 * cm, "Penerima,")

    name_y = y_ttd - 2.2 * cm

    ks_nama = settings.get("kepala_sekolah_nama") or "...................."
    bend_nama = settings.get("bendahara_nama") or "...................."

    c.setFont("Times-Bold", 11)
    c.drawString(x1, name_y, ks_nama)
    c.line(x1, name_y - 0.05 * cm, x1 + col_w - 0.6 * cm, name_y - 0.05 * cm)

    c.drawString(x2, name_y, bend_nama)
    c.line(x2, name_y - 0.05 * cm, x2 + col_w - 0.6 * cm, name_y - 0.05 * cm)

    c.drawString(x3, name_y, "....................")
    c.line(x3, name_y - 0.05 * cm, x3 + col_w - 0.6 * cm, name_y - 0.05 * cm)

    ks_nip = settings.get("kepala_sekolah_nip") or "...................."
    bend_nip = settings.get("bendahara_nip") or "...................."

    c.setFont("Times-Roman", 10)
    c.drawString(x1, name_y - 0.55 * cm, f"NIP. {ks_nip}")
    c.drawString(x2, name_y - 0.55 * cm, f"NIP. {bend_nip}")
    c.drawString(x3, name_y - 0.55 * cm, "NIP. ....................")

    c.showPage()
    c.save()

    pdf = buf.getvalue()
    buf.close()
    return pdf


# =========================================================
# BAST (A4) + LAMPIRAN FOTO
# =========================================================
def buat_pdf_bast(bpu: str, df_bku: pd.DataFrame, df_detail: pd.DataFrame) -> bytes:
    settings = get_settings()
    ov = get_bpu_override(bpu)
    photos = list_bpu_photos(bpu)

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.2 * cm,
        bottomMargin=1.2 * cm,
        title=f"BAST {bpu}",
    )

    styles = getSampleStyleSheet()

    style_school = ParagraphStyle("school", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=12, alignment=1, spaceAfter=2)
    style_meta = ParagraphStyle("meta", parent=styles["Normal"], fontName="Helvetica", fontSize=9, leading=11, alignment=1, spaceAfter=2)
    style_title = ParagraphStyle("title", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=12, alignment=1, spaceAfter=6)
    style_body = ParagraphStyle("body", parent=styles["Normal"], fontName="Helvetica", fontSize=9.5, leading=12, alignment=0, spaceAfter=6)
    style_kv = ParagraphStyle("kv", parent=styles["Normal"], fontName="Helvetica", fontSize=9.5, leading=12, alignment=0)
    style_cell = ParagraphStyle("cell", parent=styles["Normal"], fontName="Helvetica", fontSize=9, leading=11, wordWrap="CJK")
    style_cell_bold = ParagraphStyle("cellb", parent=style_cell, fontName="Helvetica-Bold")

    def _esc(s: str) -> str:
        s = "" if s is None else str(s)
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def P(txt, bold=False, underline=False):
        txt = _esc(txt)
        if underline:
            txt = f"<u>{txt}</u>"
        if bold:
            txt = f"<b>{txt}</b>"
        return Paragraph(txt, style_cell_bold if bold else style_cell)

    def KV_row(label: str, value: str, bold_value=True):
        lab = _esc(label)
        val = _esc(value)
        if bold_value:
            val = f"<b>{val}</b>"
        return [Paragraph(lab, style_kv), Paragraph(":", style_kv), Paragraph(val, style_kv)]

    # --- FUNGSI PEMBANTU UNTUK LOGIKA OVERRIDE ---
    def get_val(key, default_val=""):
        return (ov.get(key) or settings.get(key) or default_val).strip()

    nama_sekolah = get_val("nama_sekolah")
    npsn = get_val("npsn")
    alamat_satdik = get_val("alamat")
    kab_kota = get_val("kab_kota")
    tahun = get_val("tahun")
    tempat_ttd = get_val("tempat_ttd", kab_kota)
    ks_nama = get_val("kepala_sekolah_nama")

    # PIHAK 1: Menggunakan logika update terbaru (Override > Settings)
    p1_nama = get_val("pihak1_nama")
    p1_jabatan = get_val("pihak1_jabatan")
    p1_perusahaan = get_val("pihak1_perusahaan")
    p1_alamat = get_val("pihak1_alamat")
    p1_telp = get_val("pihak1_telp")

    # PIHAK 2: Default kepala sekolah / satdik
    p2_nama = get_val("pihak2_nama", ks_nama)
    p2_jabatan = get_val("pihak2_jabatan", "Kepala Sekolah")
    p2_satdik = get_val("pihak2_nama_satdik", nama_sekolah)
    p2_alamat = get_val("pihak2_alamat", f"{alamat_satdik} {kab_kota}")
    p2_telp = get_val("pihak2_telp")

    def _ph(x: str) -> str:
        return x if x else "...................."

    p1_nama, p1_jabatan, p1_perusahaan, p1_alamat, p1_telp = map(_ph, [p1_nama, p1_jabatan, p1_perusahaan, p1_alamat, p1_telp])
    p2_nama, p2_satdik, p2_alamat, p2_telp = map(_ph, [p2_nama, p2_satdik, p2_alamat, p2_telp])

    # Tanggal dari BKU
    tgl_ttd = ""
    try:
        if df_bku is not None and (not df_bku.empty) and ("Tgl" in df_bku.columns):
            tgl_ttd = str(df_bku["Tgl"].iloc[0] or "").strip()
    except Exception:
        tgl_ttd = ""

    story = []
    story.append(Paragraph(nama_sekolah if nama_sekolah else "—", style_school))
    story.append(Paragraph(f"NPSN: {_esc(npsn) if npsn else '-'} • {_esc(alamat_satdik) if alamat_satdik else '-'} • {_esc(kab_kota) if kab_kota else '-'}", style_meta))
    story.append(Paragraph("BERITA ACARA SERAH TERIMA (BAST)", style_title))
    story.append(Paragraph(f"Nomor BPU: <b>{_esc(bpu)}</b> &nbsp;&nbsp; Tahun: <b>{_esc(tahun) if tahun else '-'}</b>", style_meta))
    story.append(Spacer(1, 8))

    story.append(Paragraph("Kami yang tercantum di bawah ini:", style_body))

    pihak_tbl = Table(
        [
            [Paragraph("<b>1.</b>", style_kv), Paragraph("<b>PIHAK PERTAMA (Menyerahkan)</b>", style_kv), "", ""],
            ["", *KV_row("Nama", p1_nama)],
            ["", *KV_row("Jabatan", p1_jabatan)],
            ["", *KV_row("Nama Perusahaan", p1_perusahaan)],
            ["", *KV_row("Alamat Perusahaan", p1_alamat)],
            ["", *KV_row("No. Telepon", p1_telp)],
            ["", Spacer(1, 6), "", ""],
            [Paragraph("<b>2.</b>", style_kv), Paragraph("<b>PIHAK KEDUA (Menerima)</b>", style_kv), "", ""],
            ["", *KV_row("Nama", p2_nama)],
            ["", *KV_row("Jabatan", p2_jabatan)],
            ["", *KV_row("Nama Satdik", p2_satdik)],
            ["", *KV_row("Alamat Satdik", p2_alamat)],
            ["", *KV_row("No. Telepon", p2_telp)],
        ],
        colWidths=[0.7 * cm, 4.2 * cm, 0.5 * cm, 11.6 * cm],
    )
    pihak_tbl.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                ("TOPPADDING", (0, 0), (-1, -1), 1),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ("SPAN", (1, 0), (3, 0)),
                ("SPAN", (1, 7), (3, 7)),
                ("ALIGN", (2, 0), (2, -1), "CENTER"),
            ]
        )
    )
    story.append(pihak_tbl)
    story.append(Spacer(1, 6))

    story.append(Paragraph(
        "PIHAK PERTAMA menyerahkan hasil pekerjaan Pengadaan Barang/Jasa melalui mitra "
        f"{_esc(p1_perusahaan)} kepada PIHAK KEDUA, dan PIHAK KEDUA telah menerima hasil pekerjaan tersebut "
        "dalam jumlah yang lengkap dan kondisi yang baik sesuai dengan rincian berikut:",
        style_body
    ))
    story.append(Spacer(1, 6))

    # tabel rincian
    items = []
    if df_detail is not None and (not df_detail.empty):
        tmp = df_detail.copy()
        if "Uraian" not in tmp.columns and "[Uraian]" in tmp.columns:
            tmp["Uraian"] = tmp["[Uraian]"]
        if "Jumlah Barang" not in tmp.columns and "[Jumlah Barang]" in tmp.columns:
            tmp["Jumlah Barang"] = tmp["[Jumlah Barang]"]

        tmp["Uraian"] = tmp.get("Uraian", "").astype(str).fillna("")
        qty = pd.to_numeric(tmp.get("Jumlah Barang", 0), errors="coerce").fillna(0).astype(int)

        for i, (ura, q) in enumerate(zip(tmp["Uraian"].tolist(), qty.tolist()), start=1):
            items.append({"no": i, "barang": ura, "dipesan": q, "baik": q, "rusak": 0})
    else:
        tmp = df_bku.copy() if df_bku is not None else pd.DataFrame()
        if not tmp.empty:
            tmp["Uraian"] = tmp.get("Uraian", "").astype(str).fillna("")
            for i, ura in enumerate(tmp["Uraian"].tolist(), start=1):
                items.append({"no": i, "barang": ura, "dipesan": "", "baik": "", "rusak": ""})

    data_tbl = [
        [P("No", True), P("Barang/Jasa", True), P("Jumlah Dipesan", True),
         P("Jumlah Diterima\nKondisi Baik", True), P("Jumlah Diterima\nKondisi Rusak", True)]
    ]
    for it in items:
        data_tbl.append([P(it["no"]), P(it["barang"]), P(it["dipesan"]), P(it["baik"]), P(it["rusak"])])

    tbl = Table(
        data_tbl,
        colWidths=[1.0 * cm, 8.0 * cm, 3.0 * cm, 3.0 * cm, 3.0 * cm],
        repeatRows=1
    )
    tbl.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (0, 0), (0, -1), "CENTER"),
        ("ALIGN", (2, 1), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 10))

    story.append(Paragraph(
        "Berita Acara Serah Terima ini berfungsi sebagai bukti serah terima hasil pekerjaan kepada PIHAK KEDUA, "
        "untuk selanjutnya dicatat pada buku penerimaan barang sekolah. Demikian Berita Acara Serah Terima ini "
        "dibuat dengan sebenarnya untuk dipergunakan sebagaimana seharusnya.",
        style_body
    ))
    story.append(Spacer(1, 14))

    # TTD
    ttd_style = ParagraphStyle("ttd", parent=styles["Normal"], fontName="Helvetica", fontSize=10, leading=12, alignment=1)
    ttd = Table(
        [
            [Paragraph(f"{_esc(tempat_ttd)}, { _esc(tgl_ttd) if tgl_ttd else '....................' }", ttd_style), ""],
            ["", ""],
            [Paragraph("<b>PIHAK PERTAMA</b>", ttd_style), Paragraph("<b>PIHAK KEDUA</b>", ttd_style)],
            ["", ""], ["", ""], ["", ""],
            [
                Paragraph(f"<b><u>{_esc(p1_nama)}</u></b><br/>{_esc(p1_jabatan)}", ttd_style),
                Paragraph(f"<b><u>{_esc(p2_nama)}</u></b><br/>{_esc(p2_jabatan)}", ttd_style),
            ],
        ],
        colWidths=[9.0 * cm, 9.0 * cm],
    )
    ttd.setStyle(TableStyle([
        ("SPAN", (0, 0), (1, 0)),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(ttd)

    # Lampiran foto (halaman berikut)
    if photos:
        story.append(PageBreak())
        story.append(Paragraph(f"DOKUMENTASI FOTO BARANG - { _esc(bpu) }", style_title))
        story.append(Spacer(1, 8))

        max_w = 16.0 * cm
        max_h = 22.0 * cm

        for ph in photos:
            fn = ph.get("filename") or ""
            img_path = os.path.join(STATIC_PHOTO_DIR, fn)
            if not fn or not os.path.exists(img_path):
                continue

            story.append(Paragraph(f"Foto: {_esc(fn)}", style_meta))
            story.append(Spacer(1, 4))

            try:
                im = Image(img_path)
                iw, ih = float(im.imageWidth), float(im.imageHeight)

                scale = min(max_w / iw, max_h / ih)
                im.drawWidth = iw * scale
                im.drawHeight = ih * scale

                story.append(im)
            except Exception:
                story.append(Paragraph("(Gagal memuat gambar)", style_body))

            story.append(Spacer(1, 10))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


# =========================================================
# PUBLIC API
# =========================================================
def make_bast_pdf(bpu: str, df_bku: pd.DataFrame, df_detail: pd.DataFrame) -> bytes:
    return buat_pdf_bast(bpu=bpu, df_bku=df_bku, df_detail=df_detail)


def make_bkp_pdf(
    bpu: str,
    tgl: str,
    untuk_pembayaran: str,
    jumlah: float,
    telah_terima_dari: str,
) -> bytes:
    return buat_pdf_kwitansi(
        bpu=bpu,
        data={
            "nomor": bpu,
            "tgl": tgl,
            "telah_terima_dari": telah_terima_dari,
            "untuk_pembayaran": untuk_pembayaran,
            "jumlah": jumlah,
        },
    )