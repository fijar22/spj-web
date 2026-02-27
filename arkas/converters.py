from __future__ import annotations

import os
import pandas as pd
import pdfplumber


def clean_rek(s: str) -> str:
    return str(s).replace("\n", "").replace(" ", "").strip()


def to_num_id(x):
    # format: 1.234.567,89 -> 1234567.89
    return pd.to_numeric(str(x).replace(".", "").replace(",", "."), errors="coerce")


def to_num_plain(x):
    # format: 1.234.567 -> 1234567
    return pd.to_numeric(str(x).replace(".", "").replace(",", ""), errors="coerce")


def gabung_id_barang(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kasus ID Barang kadang terpotong jadi 2 baris.
    Jika baris berikutnya hanya digit, digabung.
    """
    if df.empty or "ID Barang" not in df.columns:
        return df

    rows = []
    skip_next = False
    for i in range(len(df)):
        if skip_next:
            skip_next = False
            continue

        id_barang = str(df.loc[i, "ID Barang"]).strip()

        if i + 1 < len(df):
            next_id = str(df.loc[i + 1, "ID Barang"]).strip()
            if next_id.replace(" ", "").isdigit():
                id_barang = (id_barang + next_id).strip()
                skip_next = True

        row = df.loc[i].copy()
        row["ID Barang"] = id_barang
        rows.append(row)

    return pd.DataFrame(rows)


def convert_bku_pdfs(pdf_paths: list[str]) -> pd.DataFrame:
    all_data: list[pd.DataFrame] = []

    for path in pdf_paths:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    df = pd.DataFrame(table)
                    if df.shape[1] < 5:
                        continue

                    df = df.iloc[:, :8]
                    df.columns = ["Tgl", "Keg", "Rek", "Bukti", "Uraian", "In", "Out", "Saldo"][: df.shape[1]]

                    # buang header baris
                    df = df[~df["Tgl"].astype(str).str.contains("Tanggal", na=False)]

                    # hanya ambil transaksi BPU
                    df = df[df["Bukti"].astype(str).str.contains("BPU", na=False)]

                    all_data.append(df)

    if not all_data:
        return pd.DataFrame(columns=["Tgl", "Keg", "Rek", "Bukti", "Uraian", "In", "Out", "Saldo"])

    df_final = pd.concat(all_data, ignore_index=True)
    df_final["Rek"] = df_final["Rek"].astype(str).apply(clean_rek)

    for col in ["In", "Out", "Saldo"]:
        if col in df_final.columns:
            df_final[col] = df_final[col].apply(to_num_id).fillna(0)

    # rapikan
    for col in ["Tgl", "Keg", "Rek", "Bukti", "Uraian"]:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str).str.strip()

    return df_final


def convert_bhp_pdfs(pdf_paths: list[str]) -> pd.DataFrame:
    all_data: list[pd.DataFrame] = []

    for path in pdf_paths:
        nama_file = os.path.basename(path)

        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    df = pd.DataFrame(table)
                    if df.shape[1] < 9:
                        continue

                    df = df.iloc[:, :9]
                    df.columns = [
                        "Tanggal",
                        "Kode Kegiatan",
                        "Kode Rekening",
                        "No Bukti",
                        "ID Barang",
                        "Uraian",
                        "Jumlah Barang",
                        "Harga Satuan",
                        "Realisasi",
                    ]

                    # buang header baris
                    df = df[~df["Tanggal"].astype(str).str.contains("Tanggal", na=False)]
                    df = df[~df["Tanggal"].astype(str).str.contains("Jumlah", na=False)]

                    df["Sumber Data"] = nama_file
                    all_data.append(df)

    if not all_data:
        return pd.DataFrame(
            columns=[
                "Tanggal",
                "Kode Kegiatan",
                "Kode Rekening",
                "No Bukti",
                "ID Barang",
                "Uraian",
                "Jumlah Barang",
                "Harga Satuan",
                "Realisasi",
                "Sumber Data",
            ]
        )

    df_final = pd.concat(all_data, ignore_index=True)

    df_final["ID Barang"] = df_final["ID Barang"].astype(str).str.strip()
    df_final = gabung_id_barang(df_final)
    df_final["ID Barang"] = (
        df_final["ID Barang"].astype(str).str.replace("\n", "", regex=False).str.replace(" ", "", regex=False)
    )

    for col in ["Jumlah Barang", "Harga Satuan", "Realisasi"]:
        if col in df_final.columns:
            df_final[col] = df_final[col].apply(to_num_plain).fillna(0)

    # rapikan text kolom utama
    for col in ["Tanggal", "Kode Kegiatan", "Kode Rekening", "No Bukti", "Uraian", "Sumber Data"]:
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str).str.strip()

    return df_final