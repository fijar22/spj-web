# arkas/queries.py
from __future__ import annotations

import math
import re
import pandas as pd
from flask import request

from .db import get_conn


# =========================================================
# HELPERS
# =========================================================
def parse_dates(series: pd.Series) -> pd.Series:
    # Kept for compatibility, but date filtering is done in SQL for correct pagination.
    return pd.to_datetime(series.astype(str).str.strip(), errors="coerce", dayfirst=True)


def get_filter_options():
    """Ambil opsi dropdown dari master (lebih stabil)."""
    conn = get_conn()
    try:
        keg = pd.read_sql(
            """
            SELECT DISTINCT nama_kegiatan
            FROM master_kegiatan
            WHERE nama_kegiatan IS NOT NULL AND TRIM(nama_kegiatan) <> ''
            ORDER BY nama_kegiatan
            """,
            conn,
        )
        rek = pd.read_sql(
            """
            SELECT DISTINCT rekap_rekening_belanja
            FROM master_rekening
            WHERE rekap_rekening_belanja IS NOT NULL AND TRIM(rekap_rekening_belanja) <> ''
            ORDER BY rekap_rekening_belanja
            """,
            conn,
        )
        kegiatan_list = keg["nama_kegiatan"].dropna().tolist() if not keg.empty else []
        rekap_list = rek["rekap_rekening_belanja"].dropna().tolist() if not rek.empty else []
        return kegiatan_list, rekap_list
    finally:
        conn.close()


# =========================================================
# BULAN OPTIONS (BKU Tgl biasanya dd-mm-yyyy)
# =========================================================
def get_bulan_options():
    conn = get_conn()
    try:
        sql = """
        SELECT DISTINCT
            (substr(TRIM(CAST(b.[Tgl] AS TEXT)), 7, 4) || '-' ||
             substr(TRIM(CAST(b.[Tgl] AS TEXT)), 4, 2)) AS ym
        FROM bku b
        WHERE b.[Bukti] LIKE 'BPU%'
          AND b.[Tgl] IS NOT NULL
          AND TRIM(CAST(b.[Tgl] AS TEXT)) <> ''
          AND length(TRIM(CAST(b.[Tgl] AS TEXT))) >= 10
        ORDER BY ym DESC
        """
        df = pd.read_sql(sql, conn)
        return df["ym"].dropna().astype(str).str.strip().tolist() if not df.empty else []
    finally:
        conn.close()


# =========================================================
# SQLITE DATE (support multiple formats) - ROBUST
# =========================================================
def _sqlite_date_expr(col: str) -> str:
    """Return SQLite DATE() expression from text date in a column."""
    c = f"TRIM(CAST({col} AS TEXT))"
    return (
        f"CASE "
        f"WHEN {c} GLOB '____-__-__*' THEN date(substr({c},1,10)) "
        f"WHEN {c} GLOB '__-__-____*' THEN date(substr({c},7,4)||'-'||substr({c},4,2)||'-'||substr({c},1,2)) "
        f"WHEN {c} GLOB '__/__/____*' THEN date(substr({c},7,4)||'-'||substr({c},4,2)||'-'||substr({c},1,2)) "
        f"ELSE NULL END"
    )


def _sqlite_ym_expr(col: str) -> str:
    """Return YYYY-MM expression from a text date column."""
    c = f"TRIM(CAST({col} AS TEXT))"
    return (
        f"CASE "
        f"WHEN {c} GLOB '____-__-__*' THEN substr({c},1,7) "
        f"WHEN {c} GLOB '__-__-____*' THEN (substr({c},7,4)||'-'||substr({c},4,2)) "
        f"WHEN {c} GLOB '__/__/____*' THEN (substr({c},7,4)||'-'||substr({c},4,2)) "
        f"ELSE '' END"
    )


# =========================================================
# PAGINATION
# =========================================================
def get_paging_args(default_per_page: int = 25):
    """Ambil page & per_page dari request.values. Dipakai di routes.py"""
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


def make_pagination(total_rows: int, page: int, per_page: int):
    total_pages = max(1, int(math.ceil(total_rows / float(per_page)))) if per_page else 1
    if page > total_pages:
        page = total_pages
    return {
        "page": page,
        "per_page": per_page,
        "total_rows": int(total_rows),
        "total_pages": int(total_pages),
        "has_prev": page > 1,
        "has_next": page < total_pages,
        "prev_page": page - 1 if page > 1 else 1,
        "next_page": page + 1 if page < total_pages else total_pages,
    }


# =========================================================
# BKU: JOIN + FILTER + PAGING  (FILTER TANGGAL DI SQL!)
# =========================================================
def ambil_data_bku(filters: dict, page: int, per_page: int):
    conn = get_conn()
    try:
        base_from = """
        FROM bku b
        LEFT JOIN master_kegiatan k ON b.[Keg] = k.[kode_kegiatan]
        LEFT JOIN master_rekening r ON b.[Rek] = r.[kode_rekening_belanja]
        """

        where = []
        params = []

        if filters.get("keyword"):
            where.append("b.[Bukti] LIKE ?")
            params.append(f"%{filters['keyword']}%")

        if filters.get("kegiatan") and filters["kegiatan"] != "__ALL__":
            where.append("k.[nama_kegiatan] = ?")
            params.append(filters["kegiatan"])

        if filters.get("rekap") and filters["rekap"] != "__ALL__":
            where.append("r.[rekap_rekening_belanja] = ?")
            params.append(filters["rekap"])

        # FILTER TANGGAL (pindah ke SQL supaya pagination akurat)
        tgl_from = (filters.get("tgl_from") or "").strip()
        tgl_to = (filters.get("tgl_to") or "").strip()
        if tgl_from:
            where.append(_sqlite_date_expr("b.[Tgl]") + " >= date(?)")
            params.append(tgl_from)
        if tgl_to:
            where.append(_sqlite_date_expr("b.[Tgl]") + " <= date(?)")
            params.append(tgl_to)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        total_sql = "SELECT COUNT(1) AS n " + base_from + where_sql
        total_rows = int(pd.read_sql(total_sql, conn, params=params)["n"].iloc[0])

        pagination = make_pagination(total_rows, page, per_page)
        offset = (pagination["page"] - 1) * pagination["per_page"]

        sql = (
            """
            SELECT
                b.[Tgl] AS Tgl,
                b.[Keg] AS Keg,
                k.[nama_kegiatan] AS NamaKegiatan,
                b.[Rek] AS Rek,
                r.[nama_rekening_belanja] AS NamaRekening,
                r.[rekap_rekening_belanja] AS RekapRekening,
                b.[Bukti] AS Bukti,
                b.[Uraian] AS Uraian,
                b.[In] AS [In],
                b.[Out] AS [Out],
                b.[Saldo] AS Saldo
            """
            + base_from
            + where_sql
            + """
            ORDER BY b.rowid DESC
            LIMIT ? OFFSET ?
            """
        )

        df = pd.read_sql(sql, conn, params=params + [pagination["per_page"], offset])
    finally:
        conn.close()

    summary = {
        "rows": int(total_rows),
        "total_in": float(pd.to_numeric(df.get("In", 0), errors="coerce").fillna(0).sum()) if len(df) else 0.0,
        "total_out": float(pd.to_numeric(df.get("Out", 0), errors="coerce").fillna(0).sum()) if len(df) else 0.0,
    }
    return df, summary, pagination


# =========================================================
# BHP/BHM: JOIN + FILTER + PAGING  (FILTER TANGGAL DI SQL!)
# =========================================================
def ambil_data_bhp(filters: dict, page: int, per_page: int):
    conn = get_conn()
    try:
        base_from = """
        FROM bhp_bhm b
        LEFT JOIN master_kegiatan k ON b.[Kode Kegiatan] = k.[kode_kegiatan]
        LEFT JOIN master_rekening r ON b.[Kode Rekening] = r.[kode_rekening_belanja]
        """

        where = []
        params = []

        if filters.get("keyword"):
            where.append("b.[No Bukti] LIKE ?")
            params.append(f"%{filters['keyword']}%")

        if filters.get("kegiatan") and filters["kegiatan"] != "__ALL__":
            where.append("k.[nama_kegiatan] = ?")
            params.append(filters["kegiatan"])

        if filters.get("rekap") and filters["rekap"] != "__ALL__":
            where.append("r.[rekap_rekening_belanja] = ?")
            params.append(filters["rekap"])

        # FILTER TANGGAL (pindah ke SQL supaya pagination akurat)
        tgl_from = (filters.get("tgl_from") or "").strip()
        tgl_to = (filters.get("tgl_to") or "").strip()
        if tgl_from:
            where.append(_sqlite_date_expr("b.[Tanggal]") + " >= date(?)")
            params.append(tgl_from)
        if tgl_to:
            where.append(_sqlite_date_expr("b.[Tanggal]") + " <= date(?)")
            params.append(tgl_to)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        total_sql = "SELECT COUNT(1) AS n " + base_from + where_sql
        total_rows = int(pd.read_sql(total_sql, conn, params=params)["n"].iloc[0])

        pagination = make_pagination(total_rows, page, per_page)
        offset = (pagination["page"] - 1) * pagination["per_page"]

        sql = (
            """
            SELECT
                b.[Tanggal] AS Tanggal,
                b.[Kode Kegiatan] AS [Kode Kegiatan],
                k.[nama_kegiatan] AS NamaKegiatan,
                b.[Kode Rekening] AS [Kode Rekening],
                r.[nama_rekening_belanja] AS NamaRekening,
                r.[rekap_rekening_belanja] AS RekapRekening,
                b.[No Bukti] AS [No Bukti],
                b.[ID Barang] AS [ID Barang],
                b.[Uraian] AS Uraian,
                b.[Jumlah Barang] AS [Jumlah Barang],
                b.[Harga Satuan] AS [Harga Satuan],
                b.[Realisasi] AS Realisasi,
                b.[Sumber Data] AS [Sumber Data]
            """
            + base_from
            + where_sql
            + """
            ORDER BY b.rowid DESC
            LIMIT ? OFFSET ?
            """
        )

        df = pd.read_sql(sql, conn, params=params + [pagination["per_page"], offset])
    finally:
        conn.close()

    jumlah_barang = pd.to_numeric(df.get("Jumlah Barang", 0), errors="coerce").fillna(0) if len(df) else pd.Series([0])
    realisasi = pd.to_numeric(df.get("Realisasi", 0), errors="coerce").fillna(0) if len(df) else pd.Series([0])

    summary = {
        "rows": int(total_rows),
        "total_jumlah_barang": float(jumlah_barang.sum()) if len(df) else 0.0,
        "total_realisasi": float(realisasi.sum()) if len(df) else 0.0,
    }
    return df, summary, pagination


# =========================================================
# SPJ per BPU (1 baris = 1 BPU), TotalOut = SUM Out
# =========================================================
def ambil_spj_per_bpu(filters: dict, page: int, per_page: int):
    conn = get_conn()
    try:
        base_from = """
        FROM bku b
        LEFT JOIN master_kegiatan k ON b.[Keg] = k.[kode_kegiatan]
        LEFT JOIN master_rekening r ON b.[Rek] = r.[kode_rekening_belanja]
        WHERE b.[Bukti] LIKE 'BPU%'
        """

        where = []
        params = []

        if filters.get("keyword"):
            where.append("b.[Bukti] LIKE ?")
            params.append(f"%{filters['keyword']}%")

        if filters.get("kegiatan") and filters["kegiatan"] != "__ALL__":
            where.append("k.[nama_kegiatan] = ?")
            params.append(filters["kegiatan"])

        if filters.get("rekap") and filters["rekap"] != "__ALL__":
            where.append("r.[rekap_rekening_belanja] = ?")
            params.append(filters["rekap"])

        # FILTER BULAN (YYYY-MM)
        bulan = (filters.get("bulan") or "").strip()
        if bulan and bulan != "__ALL__":
            where.append(
                "(substr(TRIM(CAST(b.[Tgl] AS TEXT)),7,4) || '-' || "
                "substr(TRIM(CAST(b.[Tgl] AS TEXT)),4,2)) = ?"
            )
            params.append(bulan)

        where_sql = (" AND " + " AND ".join(where)) if where else ""

        total_sql = """
        SELECT COUNT(1) AS n
        FROM (
            SELECT b.[Bukti]
        """ + base_from + where_sql + """
            GROUP BY b.[Bukti], k.[nama_kegiatan], r.[rekap_rekening_belanja]
        ) t
        """
        total_rows = int(pd.read_sql(total_sql, conn, params=params)["n"].iloc[0])

        pagination = make_pagination(total_rows, page, per_page)
        offset = (pagination["page"] - 1) * pagination["per_page"]

        sql = """
        SELECT
            b.[Bukti] AS Bukti,
            MIN(b.[Tgl]) AS Tgl,
            MIN(b.[Keg]) AS Keg,
            k.[nama_kegiatan] AS NamaKegiatan,
            MIN(b.[Rek]) AS Rek,
            r.[rekap_rekening_belanja] AS RekapRekening,
            GROUP_CONCAT(DISTINCT b.[Uraian]) AS UraianGabung,
            SUM(CAST(b.[Out] AS REAL)) AS TotalOut
        """ + base_from + where_sql + """
        GROUP BY b.[Bukti], k.[nama_kegiatan], r.[rekap_rekening_belanja]
        ORDER BY CAST(REPLACE(b.[Bukti], 'BPU', '') AS INTEGER) ASC
        LIMIT ? OFFSET ?
        """
        df = pd.read_sql(sql, conn, params=params + [pagination["per_page"], offset])
    finally:
        conn.close()

    if not df.empty and "UraianGabung" in df.columns:
        df["UraianGabung"] = df["UraianGabung"].fillna("").astype(str).str.replace(",", " | ")

    summary = {
        "rows": int(total_rows),
        "total_out": float(pd.to_numeric(df.get("TotalOut", 0), errors="coerce").fillna(0).sum()) if len(df) else 0.0,
    }
    return df, summary, pagination


# =========================================================
# DATA UNTUK DETAIL BPU (BAST PAGE / PDF)
# =========================================================
def get_bpu_bku_rows(bpu: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql(
            """
            SELECT
                b.[Tgl] AS Tgl,
                b.[Keg] AS Keg,
                k.[nama_kegiatan] AS NamaKegiatan,
                b.[Rek] AS Rek,
                r.[nama_rekening_belanja] AS NamaRekening,
                r.[rekap_rekening_belanja] AS RekapRekening,
                b.[Uraian] AS Uraian,
                b.[Out] AS Out
            FROM bku b
            LEFT JOIN master_kegiatan k ON b.[Keg] = k.[kode_kegiatan]
            LEFT JOIN master_rekening r ON b.[Rek] = r.[kode_rekening_belanja]
            WHERE b.[Bukti] = ?
            ORDER BY b.rowid ASC
            """,
            conn,
            params=[bpu],
        )
    finally:
        conn.close()


def get_bpu_bhp_detail(bpu: str) -> pd.DataFrame:
    conn = get_conn()
    try:
        return pd.read_sql(
            """
            SELECT
                [ID Barang] AS [ID Barang],
                [Uraian] AS [Uraian],
                [Jumlah Barang] AS [Jumlah Barang],
                [Harga Satuan] AS [Harga Satuan],
                [Realisasi] AS [Realisasi],
                [Sumber Data] AS [Sumber Data]
            FROM bhp_bhm
            WHERE [No Bukti] = ?
            ORDER BY rowid ASC
            """,
            conn,
            params=[bpu],
        )
    except Exception:
        return pd.DataFrame(
            columns=["ID Barang", "Uraian", "Jumlah Barang", "Harga Satuan", "Realisasi", "Sumber Data"]
        )
    finally:
        conn.close()
