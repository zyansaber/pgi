# -*- coding: utf-8 -*-
"""
Rebuild Firebase to legacy paths & format (+ Sales Order total NETWR + VIN + handover bill-to customer)

变更点（按你的要求）：
1) 不导出 Excel
2) 不删 /pgistock（已经不用了）
3) 写 /pgirecord 时：如果现有 /pgirecord/<chassis> 有 history:true，则该 chassis 完全不更新、不删除
   - 仍然会“重建”非-history 的 pgirecord：删除旧的非-history 且不在本次数据里的 chassis
4) /handover 的 createdAt/handoverAt 改成 dd/mm/yyyy（不再写 ISO 时间）
5) /handover.customer 写：门店 PGI 的 sales order (VBELN) 对应 Bill-to 名称（VBPA PARVW='RE'）
6) ✅ 新增写入 /pgirecord 字段：poNumber, vendorName, poPrice, grDateLast, grStatus
   - 注意：写 pgirecord 时采用“多路径 update”只更新指定字段，不会覆盖你之前写入的其它字段
"""

from __future__ import annotations

import io
import os
import re
import sys
import logging
import argparse
from datetime import datetime, date
from typing import Iterable, List, Optional, Dict, Any, Set

import pandas as pd
import requests
import pyodbc
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import firebase_admin
from firebase_admin import credentials, db

# ---------- logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("rebuild_pgirecord_yardstock")

# ---------- constants ----------

HANA_SERVERNODE = "10.11.2.25:30241"
HANA_UID = "BAOJIANFENG"        # <-- 改这里
HANA_PWD = "Xja@2025ABC"        # <-- 改这里

DSN = (
    "DRIVER={HDBODBC};"
    f"SERVERNODE={HANA_SERVERNODE};"
    f"UID={HANA_UID};"
    f"PWD={HANA_PWD};"
)

ORDERLIST_DL = (
    "https://regentrv-my.sharepoint.com/:x:/g/personal/"
    "planning_regentrv_com_au/ETevaCJOE_ZLqQt1ZH4mcUkBm_zrJBIN5TrKkx6tRn-7_w"
    "?e=cff2ie&download=1"
)

FIREBASE_SA_PATH = "firebase-adminsdk.json"
FIREBASE_DB_URL  = "https://scheduling-dd672-default-rtdb.asia-southeast1.firebasedatabase.app"

SPECIAL_DEALERS = {"St James", "Traralgon", "Frankston", "Geelong", "Launceston"}

# ---------- SQL ----------
SQL_PGI = r"""
SELECT DISTINCT obj.SERNR, obj.VBELN, mseg.MBLNR, mseg.BUDAT_MKPF
FROM (
  SELECT DISTINCT KDAUF, MBLNR, BUDAT_MKPF
  FROM "SAPHANADB"."NSDM_V_MSEG"
  WHERE KDPOS = 10 AND WERKS = 3111 AND BWART = '601'
    AND CONCAT(MBLNR, ZEILE) NOT IN (
      SELECT DISTINCT CONCAT(SMBLN, SMBLP) FROM "SAPHANADB"."NSDM_V_MSEG"
    )
) AS mseg
INNER JOIN (
  SELECT DISTINCT VBAK.VBELN, OBJK.SERNR
  FROM "SAPHANADB"."VBAK"
  LEFT JOIN "SAPHANADB"."SER02"
    ON VBAK.VBELN = "SAPHANADB"."SER02".SDAUFNR AND "SAPHANADB"."SER02".POSNR = 10
  LEFT JOIN "SAPHANADB"."OBJK"
    ON "SAPHANADB"."SER02".OBKNR = "SAPHANADB"."OBJK".OBKNR
  WHERE "SAPHANADB"."SER02".POSNR = 10
) AS obj
  ON mseg.KDAUF = obj.VBELN
"""

SQL_PGI_STORE = r"""
SELECT DISTINCT obj.SERNR, obj.VBELN, mseg.MBLNR, mseg.BUDAT_MKPF, mseg.WERKS, mseg.LGORT
FROM (
  SELECT DISTINCT KDAUF, MBLNR, BUDAT_MKPF, WERKS, LGORT
  FROM "SAPHANADB"."NSDM_V_MSEG"
  WHERE WERKS IN ('3211','3411') AND BWART = '601'
    AND CONCAT(MBLNR, ZEILE) NOT IN (
      SELECT DISTINCT CONCAT(SMBLN, SMBLP) FROM "SAPHANADB"."NSDM_V_MSEG"
    )
) AS mseg
INNER JOIN (
  SELECT DISTINCT VBAK.VBELN, OBJK.SERNR
  FROM "SAPHANADB"."VBAK"
  LEFT JOIN "SAPHANADB"."SER02"
    ON VBAK.VBELN = "SAPHANADB"."SER02".SDAUFNR AND "SAPHANADB"."SER02".POSNR = 10
  LEFT JOIN "SAPHANADB"."OBJK"
    ON "SAPHANADB"."SER02".OBKNR = "SAPHANADB"."OBJK".OBKNR
  WHERE "SAPHANADB"."SER02".POSNR = 10
) AS obj
  ON mseg.KDAUF = obj.VBELN
"""

SQL_STOCK = r'''
SELECT DISTINCT 
    vbak."VBELN" AS "销售订单号", 
    vbak."VDATU" AS "需求交货日期",
    objk."SERNR" AS "序列号",
    nsmka."MATNR" AS "物料号",
    SUBSTRING(makt."MAKTX", 1, 5) AS "Model Year",
    SUBSTRING(makt."MAKTX", 6)    AS "Model",
    nsmka."ERSDA" AS "创建日期",
    nsmka."KALAB" AS "库存数量",
    CASE 
        WHEN nsmka."WERKS" = '3211' AND nsmka."LGORT" = '0002' THEN 'St James'
        WHEN nsmka."WERKS" = '3211' AND nsmka."LGORT" = '0004' THEN 'Traralgon'
        WHEN nsmka."WERKS" = '3211' AND nsmka."LGORT" = '0006' THEN 'Launceston'
        WHEN nsmka."WERKS" = '3211' AND nsmka."LGORT" = '0008' THEN 'Geelong'
        WHEN nsmka."WERKS" = '3411' AND nsmka."LGORT" IN ('0002','0099') THEN 'Frankston'
        ELSE 'Unknown' 
    END AS "Location Name",
    mseg."BWART" AS "移动类型"
FROM "SAPHANADB"."VBAK" vbak
LEFT JOIN "SAPHANADB"."SER02" ser02
       ON vbak."VBELN" = ser02."SDAUFNR"
      AND ser02."POSNR" = '000010'
LEFT JOIN "SAPHANADB"."OBJK" objk
       ON ser02."OBKNR" = objk."OBKNR"
LEFT JOIN "SAPHANADB"."NSDM_V_MSKA" nsmka
       ON vbak."VBELN" = nsmka."VBELN"
LEFT JOIN "SAPHANADB"."MAKT" makt
       ON makt."MATNR" = nsmka."MATNR"
      AND makt."SPRAS" = 'E'
LEFT JOIN "SAPHANADB"."MSEG" mseg
       ON mseg."MATNR" = nsmka."MATNR"
WHERE 
    ser02."SDAUFNR" IS NOT NULL
    AND nsmka."WERKS" IN ('3211', '3411')
    AND nsmka."LGORT" IN ('0002', '0004', '0006', '0008', '0099')
    AND nsmka."KALAB" <> 0
    AND nsmka."MATNR" LIKE 'Z12%'
ORDER BY vbak."VBELN", objk."SERNR", nsmka."MATNR";
'''

# ---------- args ----------
def parse_args():
    ap = argparse.ArgumentParser(
        description="Rebuild /pgirecord and /yardstock (only 5 dealers), preserve history:true, and write handover bill-to customer."
    )
    ap.add_argument("--orderlist", help="本地 Orderlist 路径（优先使用）", default=None)
    ap.add_argument("--skip-hana", action="store_true", help="跳过 HANA（离线 PGI），配合 --sernr-csv")
    ap.add_argument("--sernr-csv", help="离线 CSV：列需含 SERNR,VBELN,BUDAT_MKPF（用于 PGI 部分）", default=None)
    ap.add_argument("--dry-run", action="store_true", help="只打印数量、不写 Firebase")
    return ap.parse_args()

# ---------- utils ----------
def sanitize_fb_key(key: str) -> str:
    return re.sub(r"[.\$\[\]#/]", "", (key or "").strip())

def dealer_key_slug(name: str) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9_-]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-")
    return s

def http_get_bytes(url: str, timeout=60) -> bytes:
    sess = requests.Session()
    retry = Retry(total=3, backoff_factor=1.0,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=["GET"])
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    headers = {"User-Agent": "Mozilla/5.0"}
    u = url.replace(" ", "%20")
    if "download=1" not in u:
        sep = "&" if "?" in u else "?"
        u = f"{u}{sep}download=1"
    resp = sess.get(u, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.content

def to_ddmmyyyy(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if re.fullmatch(r"\d{8}", s):
            dtv = datetime.strptime(s, "%Y%m%d")
        else:
            dtv = pd.to_datetime(s, errors="coerce")
            if pd.isna(dtv):
                return None
            if not isinstance(dtv, datetime):
                dtv = dtv.to_pydatetime()
        return dtv.strftime("%d/%m/%Y")
    except Exception:
        return None

def to_iso_utc_z(v) -> Optional[str]:
    # yardstock.receivedAt 你原来就是 ISO，这里保持不改
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if isinstance(v, str) and re.fullmatch(r"\d{2}/\d{2}/\d{4}", v.strip()):
            dtv = datetime.strptime(v.strip(), "%d/%m/%Y")
        else:
            if isinstance(v, date) and not isinstance(v, datetime):
                dtv = datetime(v.year, v.month, v.day)
            else:
                dtv = pd.to_datetime(v, errors="coerce")
                if pd.isna(dtv):
                    return None
                if not isinstance(dtv, datetime):
                    dtv = dtv.to_pydatetime()
        return dtv.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except Exception:
        return None

def hana_query(sql: str) -> pd.DataFrame:
    with pyodbc.connect(DSN, autocommit=True) as conn:
        return pd.read_sql(sql, conn)

def read_orderlist_df(path_or_bytes) -> pd.DataFrame:
    if isinstance(path_or_bytes, (bytes, bytearray)):
        xfile = pd.ExcelFile(io.BytesIO(path_or_bytes), engine="openpyxl")
    else:
        xfile = pd.ExcelFile(path_or_bytes, engine="openpyxl")
    sheets = xfile.sheet_names

    def norm(s): return re.sub(r"\s+", "", str(s)).lower()

    target = norm("Orderlist")
    use_sheet = next((s for s in sheets if norm(s) == target), None)
    if use_sheet is None:
        use_sheet = next((s for s in sheets if target in norm(s)), sheets[0])

    df = pd.read_excel(xfile, sheet_name=use_sheet)

    for c in ["Chassis","Dealer","Model","Customer"]:
        if c not in df.columns:
            df[c] = None

    df["Chassis_clean"] = df["Chassis"].apply(lambda x: None if pd.isna(x) else str(x).replace("-", "").strip())
    df["Customer"] = df["Customer"].fillna("Stock").astype(str).str.strip()
    df = df.sort_values(by=["Chassis_clean"], na_position="last").drop_duplicates("Chassis_clean", keep="first")
    return df[["Chassis","Chassis_clean","Dealer","Model","Customer"]].reset_index(drop=True)

# ---------- chunk helpers ----------
def _chunked(it: Iterable[str], n: int) -> Iterable[List[str]]:
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def _sql_list(values: List[str]) -> str:
    if not values:
        return "('')"
    esc = [v.replace("'", "''") for v in values]
    return "(" + ",".join(f"'{v}'" for v in esc) + ")"

# ---------- PO / Vendor / GR (by chassis from EKPO.TXZ01 prefix) ----------
def fetch_po_vendor_gr_for_chassis(chassis_list: List[str]) -> pd.DataFrame:
    """
    规则：用 EKPO.TXZ01 的第一个 token（空格前）作为 chassis 前缀匹配
    输出候选 PO item + vendor + GR 统计
    """
    chs = [c for c in pd.unique(pd.Series(chassis_list).dropna().astype(str).str.strip()) if c]
    if not chs:
        return pd.DataFrame(columns=[
            "CHASSIS", "PO_NO", "PO_ITEM", "PO_DATE",
            "VENDOR_NAME", "WAERS",
            "NETWR", "NETPR", "PEINH",
            "GR_DATE_LAST", "GR_COUNT", "GR_REV_COUNT"
        ])

    all_rows = []
    for batch in _chunked(chs, 900):
        in_list = _sql_list(batch)

        sql = f"""
        WITH ekpo_x AS (
            SELECT
                p."EBELN",
                p."EBELP",
                p."TXZ01",
                p."NETWR",
                p."NETPR",
                p."PEINH",
                SUBSTRING(
                    p."TXZ01",
                    1,
                    CASE
                        WHEN INSTR(p."TXZ01", ' ') > 0 THEN INSTR(p."TXZ01", ' ') - 1
                        ELSE LENGTH(p."TXZ01")
                    END
                ) AS "SERNR_PREFIX"
            FROM "SAPHANADB"."EKPO" p
            WHERE p."WERKS" = '3111'
              AND LOWER(p."TXZ01") LIKE '% to %'
              AND COALESCE(p."LOEKZ",'') = ''
        ),
        gr AS (
            SELECT
                "EBELN","EBELP",
                MAX(CASE WHEN "BWART" IN ('101','103','105') THEN "BUDAT_MKPF" END) AS "GR_DATE_LAST",
                COUNT(CASE WHEN "BWART" IN ('101','103','105') THEN 1 END) AS "GR_COUNT",
                COUNT(CASE WHEN "BWART" IN ('102') THEN 1 END) AS "GR_REV_COUNT"
            FROM "SAPHANADB"."NSDM_V_MSEG"
            WHERE "EBELN" IS NOT NULL
              AND "EBELP" IS NOT NULL
            GROUP BY "EBELN","EBELP"
        )
        SELECT
            ek."SERNR_PREFIX" AS "CHASSIS",
            ek."EBELN"        AS "PO_NO",
            ek."EBELP"        AS "PO_ITEM",
            ekko."BEDAT"      AS "PO_DATE",
            lfa1."NAME1"      AS "VENDOR_NAME",
            ekko."WAERS"      AS "WAERS",
            ek."NETWR"        AS "NETWR",
            ek."NETPR"        AS "NETPR",
            ek."PEINH"        AS "PEINH",
            gr."GR_DATE_LAST" AS "GR_DATE_LAST",
            gr."GR_COUNT"     AS "GR_COUNT",
            gr."GR_REV_COUNT" AS "GR_REV_COUNT"
        FROM ekpo_x ek
        LEFT JOIN "SAPHANADB"."EKKO" ekko
               ON ekko."EBELN" = ek."EBELN"
        LEFT JOIN "SAPHANADB"."LFA1" lfa1
               ON lfa1."LIFNR" = ekko."LIFNR"
        LEFT JOIN gr
               ON gr."EBELN" = ek."EBELN"
              AND gr."EBELP" = ek."EBELP"
        WHERE ek."SERNR_PREFIX" IN {in_list}
        """
        all_rows.append(hana_query(sql))

    df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=[
            "CHASSIS", "PO_NO", "PO_ITEM", "PO_DATE",
            "VENDOR_NAME", "WAERS",
            "NETWR", "NETPR", "PEINH",
            "GR_DATE_LAST", "GR_COUNT", "GR_REV_COUNT"
        ])

    for c in ["CHASSIS","PO_NO","PO_ITEM","VENDOR_NAME","WAERS"]:
        df[c] = df[c].astype("string").str.strip()

    df["PO_DATE"] = df["PO_DATE"].apply(to_ddmmyyyy)
    df["GR_DATE_LAST"] = df["GR_DATE_LAST"].apply(to_ddmmyyyy)

    return df

def pick_best_po_per_chassis(df_po: pd.DataFrame) -> pd.DataFrame:
    """每个 chassis 选 1 条：PO_DATE 最新优先"""
    if df_po is None or df_po.empty:
        return pd.DataFrame(columns=df_po.columns if df_po is not None else [])
    x = df_po.copy()
    x["_po_dt"] = pd.to_datetime(x["PO_DATE"], format="%d/%m/%Y", errors="coerce")
    x = x.sort_values(["CHASSIS","_po_dt"], ascending=[True, False], na_position="last")
    x = x.drop_duplicates("CHASSIS", keep="first").drop(columns=["_po_dt"])
    return x.reset_index(drop=True)

def compute_po_price(row: pd.Series) -> Optional[float]:
    """PO price：优先 NETWR（item 总额），否则 NETPR（单价）"""
    netwr = row.get("NETWR")
    netpr = row.get("NETPR")
    try:
        if netwr is not None and not pd.isna(netwr):
            return float(netwr)
    except Exception:
        pass
    try:
        if netpr is not None and not pd.isna(netpr):
            return float(netpr)
    except Exception:
        pass
    return None

def compute_gr_status(row: pd.Series) -> str:
    """GR 状态：有 101/103/105 => GR Posted；只有 102 => GR Reversed/Only-102；都没有 => No GR"""
    def _to_int(v) -> int:
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return 0
            return int(v)
        except Exception:
            return 0
    gr_cnt = _to_int(row.get("GR_COUNT"))
    rev_cnt = _to_int(row.get("GR_REV_COUNT"))
    if gr_cnt > 0:
        return "GR Posted"
    if rev_cnt > 0:
        return "GR Reversed/Only-102"
    return "No GR"

# ---------- price (NETWR) ----------
def fetch_salesorder_totals_3110(vbelns: List[str]) -> pd.DataFrame:
    vbelns = [v for v in pd.unique(pd.Series(vbelns).dropna().astype(str)) if v]
    if not vbelns:
        return pd.DataFrame(columns=["VBELN","total_netwr","currency"])

    all_rows = []
    for batch in _chunked(vbelns, 900):
        in_list = _sql_list(batch)
        sql = f'''
        SELECT
            vbak."VBELN",
            vbak."NETWR" AS "total_netwr",
            vbak."WAERK" AS "currency"
        FROM "SAPHANADB"."VBAK" vbak
        WHERE vbak."VKORG" = '3110'
          AND vbak."VBELN" IN {in_list}
        '''
        all_rows.append(hana_query(sql))

    df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["VBELN","total_netwr","currency"])

    df["VBELN"] = df["VBELN"].astype(str).str.strip()
    df["total_netwr"] = df["total_netwr"].apply(lambda x: None if pd.isna(x) else float(x))
    return df.drop_duplicates("VBELN", keep="last").reset_index(drop=True)

# ---------- VIN ----------
def fetch_vin_map_for_chassis(chassis_list: List[str]) -> pd.DataFrame:
    chs = [c for c in pd.unique(pd.Series(chassis_list).dropna().astype(str).str.strip()) if c]
    if not chs:
        return pd.DataFrame(columns=["SERNR","vin_number"])

    all_rows = []
    for batch in _chunked(chs, 900):
        in_list = _sql_list(batch)
        sql = f'''
        SELECT DISTINCT obj."SERNR", a."SERNR2" AS "vin_number"
        FROM "SAPHANADB"."SER02" s
        JOIN "SAPHANADB"."OBJK" obj
             ON s."OBKNR" = obj."OBKNR"
        LEFT JOIN (
            SELECT DISTINCT "SERNR","SERNR2"
            FROM "SAPHANADB"."ZTSD002"
            WHERE "WERKS" = '3091'
        ) a ON a."SERNR" = obj."SERNR"
        WHERE s."POSNR" = '000010'
          AND obj."SERNR" IN {in_list}
        '''
        all_rows.append(hana_query(sql))

    df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=["SERNR","vin_number"])
    df["SERNR"] = df["SERNR"].astype(str).str.strip()
    df["vin_number"] = df["vin_number"].astype("string").str.strip()
    return df.drop_duplicates("SERNR", keep="last").reset_index(drop=True)

# ---------- Bill-to name (handover customer) ----------
def fetch_billto_name_map(vbelns: List[str]) -> pd.DataFrame:
    """
    返回：VBELN, billto_name
    优先 Bill-to (VBPA PARVW='RE' POSNR='000000') -> KNA1.NAME1
    fallback：Sold-to (VBAK.KUNNR) -> KNA1.NAME1
    """
    vbelns = [v for v in pd.unique(pd.Series(vbelns).dropna().astype(str)) if v]
    if not vbelns:
        return pd.DataFrame(columns=["VBELN","billto_name"])

    all_rows = []
    for batch in _chunked(vbelns, 900):
        in_list = _sql_list(batch)
        sql = f'''
        SELECT
            vbak."VBELN",
            COALESCE(kre."NAME1", kag."NAME1") AS "billto_name"
        FROM "SAPHANADB"."VBAK" vbak
        LEFT JOIN "SAPHANADB"."VBPA" re
               ON re."VBELN" = vbak."VBELN"
              AND re."POSNR" = '000000'
              AND re."PARVW" = 'RE'
        LEFT JOIN "SAPHANADB"."KNA1" kre
               ON kre."KUNNR" = re."KUNNR"
        LEFT JOIN "SAPHANADB"."KNA1" kag
               ON kag."KUNNR" = vbak."KUNNR"
        WHERE vbak."VBELN" IN {in_list}
        '''
        all_rows.append(hana_query(sql))

    df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=["VBELN","billto_name"])
    if df.empty:
        return pd.DataFrame(columns=["VBELN","billto_name"])
    df["VBELN"] = df["VBELN"].astype(str).str.strip()
    df["billto_name"] = df["billto_name"].astype("string").str.strip()
    return df.drop_duplicates("VBELN", keep="last").reset_index(drop=True)

# ---------- builders ----------
def build_pgirecord_df(ser_pgi: pd.DataFrame, orderlist: pd.DataFrame) -> pd.DataFrame:
    ser = ser_pgi.copy()
    for c in ("SERNR","MBLNR","VBELN"):
        if c in ser.columns:
            ser[c] = ser[c].astype("string").str.strip()

    merged = ser.merge(
        orderlist, how="left",
        left_on="SERNR", right_on="Chassis_clean",
        suffixes=("", "_ol")
    )

    out = merged[["SERNR","BUDAT_MKPF","Dealer","Model","Customer","VBELN"]].copy()
    out = out.rename(columns={
        "SERNR":"chassis",
        "BUDAT_MKPF":"pgidate",
        "Dealer":"dealer",
        "Model":"model",
        "Customer":"customer",
        "VBELN":"vbeln",
    })

    out["pgidate"] = out["pgidate"].apply(to_ddmmyyyy)
    out["customer"] = out["customer"].fillna("Stock")

    out["_dt"] = pd.to_datetime(out["pgidate"], format="%d/%m/%Y", errors="coerce")
    out = out.sort_values(["chassis","_dt"]).drop_duplicates("chassis", keep="last").drop(columns=["_dt"])
    return out.reset_index(drop=True)

def build_yardstock_special(stock_df: pd.DataFrame, orderlist: pd.DataFrame, pgirecord_df: pd.DataFrame) -> pd.DataFrame:
    s = stock_df.copy()
    if "序列号" not in s.columns or "Location Name" not in s.columns:
        raise KeyError("库存 SQL 结果缺少 '序列号' 或 'Location Name'")

    s["Location Name"] = s["Location Name"].astype(str).str.strip()
    s = s[s["Location Name"].isin(SPECIAL_DEALERS)].copy()

    ol = orderlist[["Chassis_clean","Customer"]].copy()
    s["chassis_clean"] = s["序列号"].astype(str).str.replace(r"[-\s]", "", regex=True).str.strip()
    s = s.merge(ol, how="left", left_on="chassis_clean", right_on="Chassis_clean")
    s["Customer"] = s["Customer"].fillna("Stock")

    cols_join = ["chassis","pgidate","wholesalepo","vin_number"]
    for c in cols_join:
        if c not in pgirecord_df.columns:
            pgirecord_df[c] = None

    pgi = pgirecord_df[cols_join].rename(columns={"pgidate":"from_pgidate"})
    s = s.merge(pgi, how="left", left_on="序列号", right_on="chassis")

    out = pd.DataFrame({
        "chassis":      s["序列号"].astype(str).str.strip(),
        "dealer":       s["Location Name"].astype(str),
        "model":        s.get("Model"),
        "customer":     s["Customer"].astype(str),
        "from_pgidate": s["from_pgidate"].apply(lambda x: x if pd.notna(x) else None),
        "receivedAt":   s["创建日期"].apply(to_iso_utc_z),
        "wholesalepo":  s["wholesalepo"].apply(lambda v: None if pd.isna(v) else float(v)),
        "vin_number":   s["vin_number"].astype("string"),
    })

    out["_dt"] = pd.to_datetime(out["receivedAt"], errors="coerce")
    out = out.sort_values(["chassis","_dt"]).drop_duplicates("chassis", keep="last").drop(columns=["_dt"])
    return out.reset_index(drop=True)

def build_special_pgi_orders(ser_store: pd.DataFrame) -> pd.DataFrame:
    p = ser_store.copy()
    if p.empty:
        return pd.DataFrame(columns=["chassis","pgidate","dealer","vbeln","pgi_werks","lgort"])

    for c in ("SERNR","VBELN","BUDAT_MKPF","WERKS","LGORT"):
        if c in p.columns:
            p[c] = p[c].astype("string").str.strip()

    def _map_loc(row):
        w, l = row.get("WERKS"), row.get("LGORT")
        if w == '3211' and l == '0002': return 'St James'
        if w == '3211' and l == '0004': return 'Traralgon'
        if w == '3211' and l == '0006': return 'Launceston'
        if w == '3211' and l == '0008': return 'Geelong'
        if w == '3411' and l in ('0002','0099'): return 'Frankston'
        return f"{w}-{l}"

    p["Location Name"] = p.apply(_map_loc, axis=1)

    out = pd.DataFrame({
        "chassis":   p["SERNR"],
        "pgidate":   p["BUDAT_MKPF"].apply(to_ddmmyyyy),
        "dealer":    p["Location Name"],
        "vbeln":     p["VBELN"],
        "pgi_werks": p["WERKS"],
        "lgort":     p["LGORT"],
    })

    out["_dt"] = pd.to_datetime(out["pgidate"], format="%d/%m/%Y", errors="coerce")
    out = out.sort_values(["chassis","_dt"]).drop_duplicates("chassis", keep="last").drop(columns=["_dt"]).reset_index(drop=True)
    return out

# ---------- handover ----------
def build_handover_records(df_special_pgi_orders: pd.DataFrame,
                           df_stock: pd.DataFrame,
                           orderlist: pd.DataFrame) -> pd.DataFrame:
    """
    createdAt / handoverAt：dd/mm/yyyy
    customer：使用 df_special_pgi_orders.customer（Bill-to name），为空则 'NA'
    """
    if df_special_pgi_orders.empty:
        return pd.DataFrame(columns=[
            "dealerSlug","chassis","createdAt","handoverAt","dealerName","model","customer","source"
        ])

    df = df_special_pgi_orders.copy()
    df["createdAt"]  = df["pgidate"]
    df["handoverAt"] = df["pgidate"]
    df["dealerName"] = df["dealer"].astype(str)
    df["dealerSlug"] = df["dealerName"].apply(dealer_key_slug)

    # 尝试补 model（stock 优先，其次 orderlist）
    model_map_stock = None
    if not df_stock.empty and "序列号" in df_stock.columns and "Model" in df_stock.columns:
        model_map_stock = df_stock[["序列号","Model"]].dropna().drop_duplicates()
    df = df.merge(model_map_stock, how="left", left_on="chassis", right_on="序列号") if model_map_stock is not None else df

    if not orderlist.empty and "Chassis_clean" in orderlist.columns and "Model" in orderlist.columns:
        df = df.merge(
            orderlist[["Chassis_clean","Model"]].rename(columns={"Chassis_clean":"chassis","Model":"Model_ol"}),
            how="left", on="chassis"
        )
    else:
        df["Model_ol"] = None

    if "Model" in df.columns:
        df["model_final"] = df["Model"].where(df["Model"].notna() & (df["Model"].astype(str) != ""), df["Model_ol"])
    else:
        df["model_final"] = df["Model_ol"]

    if "customer" not in df.columns:
        df["customer"] = None
    df["customer_final"] = df["customer"].where(df["customer"].notna() & (df["customer"].astype(str).str.strip() != ""), "NA")

    out = pd.DataFrame({
        "dealerSlug": df["dealerSlug"],
        "chassis":    df["chassis"].astype(str).str.strip(),
        "createdAt":  df["createdAt"],
        "handoverAt": df["handoverAt"],
        "dealerName": df["dealerName"],
        "model":      df["model_final"],
        "customer":   df["customer_final"],
        "source":     "SAPdata",
    })
    return out[out["chassis"].astype(bool)].reset_index(drop=True)

# ---------- firebase ----------
def firebase_init():
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_SA_PATH)
        firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})

def fb_update(path: str, payload: dict):
    db.reference(path).update(payload)

# ---------- pgirecord helpers (history preserve) ----------
def fetch_pgirecord_history_true_keys() -> Set[str]:
    data = db.reference("pgirecord").get() or {}
    keys: Set[str] = set()
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, dict) and v.get("history") is True:
                keys.add(str(k))
    return keys

def delete_pgirecord_children_except(keep_keys: Set[str], batch_size: int = 2000) -> int:
    data = db.reference("pgirecord").get() or {}
    if not isinstance(data, dict) or not data:
        return 0
    existing_keys = set(data.keys())
    to_delete = sorted(existing_keys - keep_keys)
    if not to_delete:
        return 0

    deleted = 0
    for batch in _chunked(to_delete, batch_size):
        payload = {k: None for k in batch}
        db.reference("pgirecord").update(payload)
        deleted += len(batch)
    return deleted

def _has_value(v) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and pd.isna(v):
        return False
    if isinstance(v, str) and not v.strip():
        return False
    return True

# ---------- writers ----------
def write_pgirecord_preserve_history(df: pd.DataFrame) -> int:
    """
    ✅ 只更新指定字段（多路径 update），不会覆盖你之前写入的其它字段
    - history:true chassis 跳过
    - 不在本次数据且非 history:true 的 chassis 会被删除
    """
    history_true_keys = fetch_pgirecord_history_true_keys()
    log.info("pgirecord: history:true chassis count = %d", len(history_true_keys))

    # 多路径 update：{ "CHASSIS/pgidate": "...", "CHASSIS/dealer": "...", ... }
    multi_update: Dict[str, Any] = {}
    updated_keys: Set[str] = set()

    for _, r in df.iterrows():
        key = sanitize_fb_key((r.get("chassis") or "").strip())
        if not key:
            continue
        if key in history_true_keys:
            continue

        updated_keys.add(key)

        # core fields（只在有值时更新，避免把已有值覆盖成 None）
        v = r.get("pgidate")
        if _has_value(v): multi_update[f"{key}/pgidate"] = v

        v = r.get("dealer")
        if _has_value(v): multi_update[f"{key}/dealer"] = (None if pd.isna(v) else str(v))

        v = r.get("model")
        if _has_value(v): multi_update[f"{key}/model"] = (None if pd.isna(v) else str(v))

        v = r.get("customer")
        if _has_value(v): multi_update[f"{key}/customer"] = (None if pd.isna(v) else str(v))

        # wholesalepo
        if "wholesalepo" in df.columns:
            v = r.get("wholesalepo")
            if _has_value(v):
                try:
                    multi_update[f"{key}/wholesalepo"] = float(v)
                except Exception:
                    pass

        # vinNumber
        v = r.get("vin_number")
        if _has_value(v):
            multi_update[f"{key}/vinNumber"] = str(v)

        # ✅ new fields: poNumber, vendorName, poPrice, grDateLast, grStatus
        v = r.get("poNumber")
        if _has_value(v): multi_update[f"{key}/poNumber"] = str(v).strip()

        v = r.get("vendorName")
        if _has_value(v): multi_update[f"{key}/vendorName"] = str(v).strip()

        v = r.get("poPrice")
        if _has_value(v):
            try:
                multi_update[f"{key}/poPrice"] = float(v)
            except Exception:
                pass

        v = r.get("grDateLast")
        if _has_value(v): multi_update[f"{key}/grDateLast"] = str(v).strip()

        v = r.get("grStatus")
        if _has_value(v): multi_update[f"{key}/grStatus"] = str(v).strip()

    # selective rebuild：保留 = 本次 updated_keys + history_true_keys
    keep_keys = set(updated_keys) | set(history_true_keys)

    deleted = delete_pgirecord_children_except(keep_keys=keep_keys, batch_size=2000)
    if deleted:
        log.info("pgirecord: deleted old non-history keys = %d", deleted)

    if multi_update:
        db.reference("pgirecord").update(multi_update)

    return len(updated_keys)

def write_yardstock_special_dealers_only(df: pd.DataFrame, allowed_dealers: Set[str]) -> int:
    """
    ✅ 只更新 /yardstock/{dealer-slug}（SPECIAL_DEALERS）
    - 不影响其它 dealer
    - 对每个 dealerSlug 使用 set 覆盖该 dealer 子树
    """
    if df is None or df.empty:
        for dealer in sorted(allowed_dealers):
            slug = dealer_key_slug(dealer)
            db.reference(f"yardstock/{slug}").set({})
        return 0

    df2 = df.copy()
    df2["dealer"] = df2["dealer"].astype(str).str.strip()

    total = 0
    for dealer in sorted(allowed_dealers):
        slug = dealer_key_slug(dealer)
        if not slug:
            continue

        sub: Dict[str, Any] = {}
        part = df2[df2["dealer"] == dealer]
        for _, r in part.iterrows():
            chassis = sanitize_fb_key((r.get("chassis") or "").strip())
            if not chassis:
                continue
            sub[chassis] = {
                "customer":     None if pd.isna(r.get("customer")) else str(r.get("customer")),
                "dealer":       dealer,
                "from_pgidate": r.get("from_pgidate") if pd.notna(r.get("from_pgidate")) else None,
                "model":        None if pd.isna(r.get("model")) else str(r.get("model")),
                "receivedAt":   r.get("receivedAt") if pd.notna(r.get("receivedAt")) else None,
                "wholesalepo":  None if pd.isna(r.get("wholesalepo")) else float(r.get("wholesalepo")),
                "vinNumber":    None if pd.isna(r.get("vin_number")) else str(r.get("vin_number")),
            }

        db.reference(f"yardstock/{slug}").set(sub)
        total += len(sub)

    return total

def write_handover_special_dealers_only(df: pd.DataFrame, allowed_dealers: Set[str]) -> int:
    """
    ✅ 只更新 /handover/{dealer-slug}（SPECIAL_DEALERS）
    - 不影响其它 dealer
    - createdAt/handoverAt 已是 dd/mm/yyyy
    - customer 写 bill-to name
    """
    allowed_slugs = {dealer_key_slug(d) for d in allowed_dealers if dealer_key_slug(d)}

    if df is None or df.empty:
        for slug in sorted(allowed_slugs):
            db.reference(f"handover/{slug}").set({})
        return 0

    df2 = df.copy()
    df2["dealerSlug"] = df2["dealerSlug"].astype(str).str.strip()

    total = 0
    for dealer in sorted(allowed_dealers):
        slug = dealer_key_slug(dealer)
        if not slug:
            continue

        sub: Dict[str, Any] = {}
        part = df2[df2["dealerSlug"] == slug]
        for _, r in part.iterrows():
            ch = sanitize_fb_key((r.get("chassis") or "").strip())
            if not ch:
                continue
            sub[ch] = {
                "chassis": ch,
                "createdAt": r.get("createdAt"),
                "handoverAt": r.get("handoverAt"),
                "dealerName": r.get("dealerName"),
                "dealerSlug": slug,
                "model": None if pd.isna(r.get("model")) else str(r.get("model")),
                "customer": None if pd.isna(r.get("customer")) else str(r.get("customer")),
                "source": "SAPdata",
            }

        db.reference(f"handover/{slug}").set(sub)
        total += len(sub)

    return total

# ---------- main ----------
def main():
    args = parse_args()
    log.info("[flag] --skip-hana   = %s", args.skip_hana)
    log.info("[flag] --dry-run     = %s", args.dry_run)

    # ------- Orderlist -------
    try:
        if args.orderlist and os.path.exists(args.orderlist):
            log.info("读取本地 Orderlist：%s", args.orderlist)
            ol = read_orderlist_df(args.orderlist)
        else:
            log.info("下载 Orderlist（SharePoint 直链）…")
            ol = read_orderlist_df(http_get_bytes(ORDERLIST_DL))
        log.info("Orderlist 记录数：%s", len(ol))
    except Exception as e:
        log.error("Orderlist 获取失败：%s（以空表继续）", e)
        ol = pd.DataFrame(columns=["Chassis","Chassis_clean","Dealer","Model","Customer"])

    # ------- PGI 数据 -------
    try:
        if args.skip_hana:
            if not args.sernr_csv:
                raise RuntimeError("--skip-hana 需要 --sernr-csv=...（列：SERNR,VBELN,BUDAT_MKPF）")
            ser = pd.read_csv(args.sernr_csv, dtype=str).fillna("")
            for c in ["SERNR","VBELN","BUDAT_MKPF"]:
                if c not in ser.columns:
                    raise RuntimeError(f"CSV 缺少列：{c}")
            ser_factory = ser.rename(columns=str)
            ser_store   = pd.DataFrame(columns=["SERNR","VBELN","BUDAT_MKPF","WERKS","LGORT"])
            log.info("已使用离线 CSV（工厂 PGI %d 条）", len(ser_factory))
        else:
            log.info("查询 HANA（PGI 601 工厂 3111）…")
            ser_factory = hana_query(SQL_PGI)
            log.info("工厂 PGI 条数：%s", len(ser_factory))
            log.info("查询 HANA（PGI 601 门店 3211/3411）…")
            ser_store = hana_query(SQL_PGI_STORE)
            log.info("门店 PGI 条数：%s", len(ser_store))
    except Exception as e:
        log.error("PGI 查询失败：%s（以空表继续）", e)
        ser_factory = pd.DataFrame(columns=["SERNR","VBELN","BUDAT_MKPF"])
        ser_store   = pd.DataFrame(columns=["SERNR","VBELN","BUDAT_MKPF","WERKS","LGORT"])

    # ------- 构建 pgirecord -------
    try:
        df_pgirecord = build_pgirecord_df(ser_factory, ol)
    except Exception as e:
        log.error("构建 pgirecord 失败：%s（以空表继续）", e)
        df_pgirecord = pd.DataFrame(columns=["chassis","pgidate","dealer","model","customer","vbeln"])
    log.info("pgirecord 去重后：%d", len(df_pgirecord))

    # ------- 3110 销售订单整单总价 -------
    try:
        vbelns = df_pgirecord["vbeln"].dropna().astype(str).str.strip().unique().tolist() if "vbeln" in df_pgirecord.columns else []
        if vbelns and (not args.skip_hana):
            log.info("查询 3110 销售订单总价（NETWR excl GST）（%d 个订单）…", len(vbelns))
            df_total = fetch_salesorder_totals_3110(vbelns)
            if not df_total.empty:
                df_pgirecord = df_pgirecord.merge(
                    df_total[["VBELN","total_netwr","currency"]],
                    how="left", left_on="vbeln", right_on="VBELN",
                ).drop(columns=["VBELN"])
                df_pgirecord["wholesalepo"] = df_pgirecord["total_netwr"]
                df_pgirecord = df_pgirecord.drop(columns=["total_netwr"])
            else:
                df_pgirecord["wholesalepo"] = None
                df_pgirecord["currency"] = None
        else:
            df_pgirecord["wholesalepo"] = None
            df_pgirecord["currency"] = None
    except Exception as e:
        log.error("整单总价查询失败：%s（以空价继续）", e)
        df_pgirecord["wholesalepo"] = None
        df_pgirecord["currency"] = None

    # ------- VIN -------
    try:
        chassis_list = df_pgirecord["chassis"].dropna().astype(str).str.strip().unique().tolist() if "chassis" in df_pgirecord.columns else []
        if chassis_list and (not args.skip_hana):
            log.info("查询 VIN 映射（%d 个 chassis）…", len(chassis_list))
            df_vin = fetch_vin_map_for_chassis(chassis_list)
            if not df_vin.empty:
                df_vin.rename(columns={"SERNR":"chassis"}, inplace=True)
                df_pgirecord = df_pgirecord.merge(df_vin, how="left", on="chassis")
            else:
                df_pgirecord["vin_number"] = None
        else:
            df_pgirecord["vin_number"] = None
    except Exception as e:
        log.error("VIN 映射查询失败：%s（以空表继续）", e)
        df_pgirecord["vin_number"] = None

    # ------- PO / Vendor / GR -------
    try:
        chassis_list = df_pgirecord["chassis"].dropna().astype(str).str.strip().unique().tolist() if "chassis" in df_pgirecord.columns else []
        if chassis_list and (not args.skip_hana):
            log.info("查询 PO/Vendor/GR（%d 个 chassis）…", len(chassis_list))
            df_po_all = fetch_po_vendor_gr_for_chassis(chassis_list)
            df_po_best = pick_best_po_per_chassis(df_po_all)

            if not df_po_best.empty:
                df_po_best["poPrice"] = df_po_best.apply(compute_po_price, axis=1)
                df_po_best["grStatus"] = df_po_best.apply(compute_gr_status, axis=1)

                df_po_best = df_po_best.rename(columns={
                    "CHASSIS": "chassis",
                    "PO_NO": "poNumber",
                    "VENDOR_NAME": "vendorName",
                    "GR_DATE_LAST": "grDateLast",
                })

                df_po_best = df_po_best[["chassis","poNumber","vendorName","poPrice","grDateLast","grStatus"]].copy()
                df_pgirecord = df_pgirecord.merge(df_po_best, how="left", on="chassis")
            else:
                df_pgirecord["poNumber"] = None
                df_pgirecord["vendorName"] = None
                df_pgirecord["poPrice"] = None
                df_pgirecord["grDateLast"] = None
                df_pgirecord["grStatus"] = None
        else:
            df_pgirecord["poNumber"] = None
            df_pgirecord["vendorName"] = None
            df_pgirecord["poPrice"] = None
            df_pgirecord["grDateLast"] = None
            df_pgirecord["grStatus"] = None
    except Exception as e:
        log.error("PO/Vendor/GR 查询失败：%s（这些字段置空继续）", e)
        df_pgirecord["poNumber"] = None
        df_pgirecord["vendorName"] = None
        df_pgirecord["poPrice"] = None
        df_pgirecord["grDateLast"] = None
        df_pgirecord["grStatus"] = None

    # ------- STOCK -------
    try:
        if args.skip_hana:
            raise RuntimeError("skip-hana 模式：库存 SQL 不查询（以空表继续）")
        log.info("查询 HANA（库存 SQL：仅 5 门店 LGORT）…")
        df_stock = hana_query(SQL_STOCK)
        log.info("库存记录数：%s", len(df_stock))
    except Exception as e:
        log.warning("库存查询跳过/失败：%s（以空表继续）", e)
        df_stock = pd.DataFrame(columns=["序列号","Location Name","Model","创建日期"])

    # ------- yardstock 预览（只写 5 门店） -------
    try:
        df_yard_special = build_yardstock_special(df_stock, ol, df_pgirecord)
    except Exception as e:
        log.error("构建 yardstock_special 失败：%s（以空表继续）", e)
        df_yard_special = pd.DataFrame(columns=["chassis","dealer","model","customer","from_pgidate","receivedAt","wholesalepo","vin_number"])

    # ------- special_pgi_orders -------
    try:
        df_special_pgi_orders = build_special_pgi_orders(ser_store)
        log.info("special_pgi_orders(3211/3411 PGI)：%d", len(df_special_pgi_orders))
    except Exception as e:
        log.error("构建 special_pgi_orders 失败：%s（以空表继续）", e)
        df_special_pgi_orders = pd.DataFrame(columns=["chassis","pgidate","dealer","vbeln","pgi_werks","lgort"])

    # ------- Bill-to customer for handover -------
    try:
        if (not args.skip_hana) and (not df_special_pgi_orders.empty) and ("vbeln" in df_special_pgi_orders.columns):
            vbelns_store = df_special_pgi_orders["vbeln"].dropna().astype(str).str.strip().unique().tolist()
            log.info("查询 Bill-to 名称（handover.customer）（%d 个 sales order）…", len(vbelns_store))
            df_billto = fetch_billto_name_map(vbelns_store)
            if not df_billto.empty:
                df_special_pgi_orders = df_special_pgi_orders.merge(
                    df_billto, how="left", left_on="vbeln", right_on="VBELN"
                ).drop(columns=["VBELN"])
                df_special_pgi_orders.rename(columns={"billto_name": "customer"}, inplace=True)
            else:
                df_special_pgi_orders["customer"] = None
        else:
            df_special_pgi_orders["customer"] = None
    except Exception as e:
        log.error("Bill-to 名称查询失败：%s（handover.customer 置空）", e)
        df_special_pgi_orders["customer"] = None

    # ------- 写库（若非 dry-run） -------
    if args.dry_run:
        log.info("[dry-run] 仅统计，不写 Firebase：pgirecord=%d, yardstock(special)=%d, handover(依据PGI门店)=%d",
                 len(df_pgirecord), len(df_yard_special), len(df_special_pgi_orders))
        return

    firebase_init()

    log.info("⬆️ 写入 /pgirecord（保护 history:true + selective rebuild；多路径 update 不覆盖其它字段）...")
    n1 = write_pgirecord_preserve_history(df_pgirecord)
    log.info("✅ /pgirecord 本次更新 %d 条（history:true 自动跳过）", n1)

    log.info("⬆️ 更新 /yardstock（仅 5 门店；不影响其它 dealer） ...")
    n2 = write_yardstock_special_dealers_only(df_yard_special, allowed_dealers=SPECIAL_DEALERS)
    log.info("✅ /yardstock（special dealers）写入 %d 条", n2)

    try:
        df_handover = build_handover_records(df_special_pgi_orders, df_stock, ol)
        log.info("⬆️ 更新 /handover（仅 5 门店；不影响其它 dealer；日期 dd/mm/yyyy；customer=Bill-to） ...")
        n3 = write_handover_special_dealers_only(df_handover, allowed_dealers=SPECIAL_DEALERS)
        log.info("✅ /handover（special dealers）更新 %d 条", n3)
    except Exception as e:
        log.error("写入 /handover 失败：%s", e)

    log.info("🎉 完成")

if __name__ == "__main__":
    main()
