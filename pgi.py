# -*- coding: utf-8 -*-

import logging
import pandas as pd
import pyodbc

# ================= CONFIG =================

HANA_SERVERNODE = "10.11.2.25:30241"
HANA_UID = "BAOJIANFENG"
HANA_PWD = "Xja@2025ABC"

DSN = (
    "DRIVER={HDBODBC};"
    f"SERVERNODE={HANA_SERVERNODE};"
    f"UID={HANA_UID};"
    f"PWD={HANA_PWD};"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger()

# ================= COUNT LIST =================

COUNT_LIST = [
"RRV220232","RRV21116","RRV21118","RRV220138","LRV240704",
"SRC253292","SRC253317","SRC253268","SRC254562","SRC254830",
"SRC254939","LRV233305","SRC253795","SRC254056","SRC254041",
"SRC254536","SRC254940","SRC254756","SRC255458","SRC243593",
"SRC253151","SRC253826","SRC243809","SRC254501","SRC254917",
"SRC255119","SRC254890","SRC254587","SRC254999","SRC243133",
"SRC254440","SRC254550","SRC254602","SRC254537","SRC253265",
"SRC254399","SRC255261","SRC255001","SRC253942","SRC254781",
"SRC254460","SRC254575","SRC254773","SRC254787","SRC254511",
"SRH253259","SRH243743","SRH254510","SRH253701","SRH243734",
"SRH253261","SRH254642","SRH254772","SRH250051","SRH250130",
"SRH250129","SRH254549","SRP253941","SRP254377","LRP230002",
"SRP253063","SRP253295","SRP253844","SRP254608","SRP255036",
"SRP255250","SRP254755","SRP255004","SRP255003","SRT253740",
"SRT254777","SRT254466","SRT254583","LRT240090","LRT240093",
"LRT240080","SRT253271","SRT244022","SRT253071","SRT253836",
"SRT253322","SRT254920","SRT255169","SRT254551","SRT254600",
"SRT254921","SRT254981","LRT240507","LRT230482","SRT253827",
"SRT254470","SRT254592","SRT255171","SRV250013","SRP255007"
]

DETAIL_COLUMNS = [
    "Chassis",
    "Mismatch_Type",
    "SalesOrder_3120",
    "SalesOrder_3110",
    "SalesOrderPGI_Doc_3120",
    "SalesOrderPGI_Doc_3110",
    "PGI_Date_3120",
    "PGI_Date_3110",
    "Reverse_PGI_3120",
    "Reverse_PGI_3110",
    "Last_Movement_Is_PGI_3120",
    "Last_Movement_Is_PGI_3110",
    "Invoice_No_3120",
    "PO_Number_3120",
    "PO_Number_Count",
    "PO_GR_Date_3120",
    "BillTo_3110",
    "BillTo_3120",
    "BP_Received_Amount_3120",
]

# ================= DB =================

def hana_query(sql):
    with pyodbc.connect(DSN, autocommit=True) as conn:
        return pd.read_sql(sql, conn)

# ================= STEP 1: 真实库存 =================

def fetch_true_stock():

    sql_stock = """
    SELECT DISTINCT
        objk."SERNR" AS "Chassis",
        vbak."VBELN" AS "SalesOrder"

    FROM "SAPHANADB"."NSDM_V_MSKA" nsmka
    LEFT JOIN "SAPHANADB"."SER02" ser02
        ON nsmka."VBELN" = ser02."SDAUFNR"
       AND ser02."POSNR" = '000010'
    LEFT JOIN "SAPHANADB"."OBJK" objk
        ON ser02."OBKNR" = objk."OBKNR"
    LEFT JOIN "SAPHANADB"."VBAK" vbak
        ON ser02."SDAUFNR" = vbak."VBELN"

    WHERE nsmka."WERKS" = '3211'
      AND nsmka."LGORT" = '0002'
      AND nsmka."KALAB" > 0
      AND nsmka."MATNR" LIKE 'Z12%'
    """

    df_stock = hana_query(sql_stock)

    sql_move = """
    SELECT
        mseg."KDAUF" AS "SalesOrder",
        mseg."BWART",
        mseg."BUDAT_MKPF"
    FROM "SAPHANADB"."NSDM_V_MSEG" mseg
    WHERE mseg."BWART" IN ('601','602')
    """

    df_move = hana_query(sql_move)
    df_move["BUDAT_MKPF"] = pd.to_datetime(df_move["BUDAT_MKPF"])

    df_last = (
        df_move.sort_values("BUDAT_MKPF")
        .groupby("SalesOrder")
        .last()
        .reset_index()
    )

    df_stock = df_stock.merge(df_last, on="SalesOrder", how="left")

    # 删除最后是601的
    df_true = df_stock[df_stock["BWART"] != "601"]

    return set(df_true["Chassis"].dropna())


# ================= STEP 2: 对比 =================

def build_mismatch(sap_set):

    list_set = set(COUNT_LIST)
    all_serial = sorted(list_set.union(sap_set))

    rows = []

    for s in all_serial:
        if s in list_set and s in sap_set:
            continue
        if s in list_set:
            rows.append([s, "Only in List"])
        else:
            rows.append([s, "Only in SAP"])

    return pd.DataFrame(rows, columns=["Chassis", "Mismatch_Type"])


# ================= STEP 3: Mismatch 明细 =================

def fetch_mismatch_details(df_mismatch):
    if df_mismatch.empty:
        return pd.DataFrame(columns=DETAIL_COLUMNS)

    serial_list = df_mismatch["Chassis"].dropna().unique().tolist()
    in_list = "(" + ",".join(f"'{c}'" for c in serial_list) + ")"

    sql = f"""
    WITH base AS (
        SELECT DISTINCT
            obj."SERNR" AS "Chassis",
            s."SDAUFNR" AS "SalesOrder"
        FROM "SAPHANADB"."OBJK" obj
        LEFT JOIN "SAPHANADB"."SER02" s
            ON obj."OBKNR" = s."OBKNR"
        WHERE obj."SERNR" IN {in_list}
    ),
    so_3120 AS (
        SELECT b."Chassis", v."VBELN" AS "SalesOrder_3120"
        FROM base b
        JOIN "SAPHANADB"."VBAK" v
          ON b."SalesOrder" = v."VBELN"
         AND v."VKORG" = '3120'
    ),
    so_3110 AS (
        SELECT b."Chassis", v."VBELN" AS "SalesOrder_3110"
        FROM base b
        JOIN "SAPHANADB"."VBAK" v
          ON b."SalesOrder" = v."VBELN"
         AND v."VKORG" = '3110'
    ),
    move_3120 AS (
        SELECT
            s."Chassis",
            MAX(CASE WHEN m."BWART"='601' THEN m."MBLNR" END) AS "SalesOrderPGI_Doc_3120",
            MAX(CASE WHEN m."BWART"='601' THEN m."BUDAT_MKPF" END) AS "PGI_Date_3120",
            CASE WHEN SUM(CASE WHEN m."BWART"='602' THEN 1 ELSE 0 END) > 0 THEN 'Y' ELSE 'N' END AS "Reverse_PGI_3120",
            CASE WHEN MAX(m."BWART") = '601' THEN 'Y' ELSE 'N' END AS "Last_Movement_Is_PGI_3120"
        FROM so_3120 s
        LEFT JOIN "SAPHANADB"."NSDM_V_MSEG" m
          ON m."KDAUF" = s."SalesOrder_3120"
         AND m."BWART" IN ('601','602')
        GROUP BY s."Chassis"
    ),
    move_3110 AS (
        SELECT
            s."Chassis",
            MAX(CASE WHEN m."BWART"='601' THEN m."MBLNR" END) AS "SalesOrderPGI_Doc_3110",
            MAX(CASE WHEN m."BWART"='601' THEN m."BUDAT_MKPF" END) AS "PGI_Date_3110",
            CASE WHEN SUM(CASE WHEN m."BWART"='602' THEN 1 ELSE 0 END) > 0 THEN 'Y' ELSE 'N' END AS "Reverse_PGI_3110",
            CASE WHEN MAX(m."BWART") = '601' THEN 'Y' ELSE 'N' END AS "Last_Movement_Is_PGI_3110"
        FROM so_3110 s
        LEFT JOIN "SAPHANADB"."NSDM_V_MSEG" m
          ON m."KDAUF" = s."SalesOrder_3110"
         AND m."BWART" IN ('601','602')
        GROUP BY s."Chassis"
    ),
    bill_3120 AS (
        SELECT
            s."Chassis",
            MAX(vbrp."VBELN") AS "Invoice_No_3120"
        FROM so_3120 s
        LEFT JOIN "SAPHANADB"."VBRP" vbrp
          ON vbrp."AUBEL" = s."SalesOrder_3120"
        GROUP BY s."Chassis"
    ),
    po_3120 AS (
        SELECT
            s."Chassis",
            MAX(COALESCE(vbkd."BSTKD", vbak."BSTNK")) AS "PO_Number_3120",
            COUNT(DISTINCT COALESCE(vbkd."BSTKD", vbak."BSTNK")) AS "PO_Number_Count"
        FROM so_3120 s
        LEFT JOIN "SAPHANADB"."VBAK" vbak
          ON s."SalesOrder_3120" = vbak."VBELN"
        LEFT JOIN "SAPHANADB"."VBKD" vbkd
          ON vbkd."VBELN" = vbak."VBELN"
         AND vbkd."POSNR" = '000000'
        GROUP BY s."Chassis"
    ),
    gr_3120 AS (
        SELECT
            p."Chassis",
            MAX(ekbe."BUDAT") AS "PO_GR_Date_3120"
        FROM po_3120 p
        LEFT JOIN "SAPHANADB"."EKBE" ekbe
          ON ekbe."XBLNR" = p."PO_Number_3120"
         AND ekbe."VGABE" = '1'
        GROUP BY p."Chassis"
    ),
    bp_3110 AS (
        SELECT
            s."Chassis",
            MAX(vbpa."KUNNR") AS "BillTo_3110"
        FROM so_3110 s
        LEFT JOIN "SAPHANADB"."VBPA" vbpa
          ON vbpa."VBELN" = s."SalesOrder_3110"
         AND vbpa."PARVW" = 'RE'
        GROUP BY s."Chassis"
    ),
    bp_3120 AS (
        SELECT
            s."Chassis",
            MAX(vbpa."KUNNR") AS "BillTo_3120"
        FROM so_3120 s
        LEFT JOIN "SAPHANADB"."VBPA" vbpa
          ON vbpa."VBELN" = s."SalesOrder_3120"
         AND vbpa."PARVW" = 'RE'
        GROUP BY s."Chassis"
    ),
    bp_recv AS (
        SELECT
            b."Chassis",
            SUM(bsad."DMBTR") AS "BP_Received_Amount_3120"
        FROM bp_3120 b
        LEFT JOIN "SAPHANADB"."BSAD" bsad
          ON bsad."KUNNR" = b."BillTo_3120"
        GROUP BY b."Chassis"
    )
    SELECT
        mm."Chassis",
        mm."Mismatch_Type",
        s3120."SalesOrder_3120",
        s3110."SalesOrder_3110",
        m3120."SalesOrderPGI_Doc_3120",
        m3110."SalesOrderPGI_Doc_3110",
        m3120."PGI_Date_3120",
        m3110."PGI_Date_3110",
        m3120."Reverse_PGI_3120",
        m3110."Reverse_PGI_3110",
        m3120."Last_Movement_Is_PGI_3120",
        m3110."Last_Movement_Is_PGI_3110",
        b3120."Invoice_No_3120",
        p3120."PO_Number_3120",
        p3120."PO_Number_Count",
        g3120."PO_GR_Date_3120",
        bp3110."BillTo_3110",
        bp3120."BillTo_3120",
        recv."BP_Received_Amount_3120"
    FROM (
        SELECT '{"','".join(serial_list)}' AS "_tmp"
    ) t
    JOIN (
        SELECT * FROM (
            VALUES {','.join([f"('{c}', '{t}')" for c, t in df_mismatch[["Chassis", "Mismatch_Type"]].itertuples(index=False, name=None)])}
        ) AS v("Chassis", "Mismatch_Type")
    ) mm ON 1=1
    LEFT JOIN so_3120 s3120 ON mm."Chassis" = s3120."Chassis"
    LEFT JOIN so_3110 s3110 ON mm."Chassis" = s3110."Chassis"
    LEFT JOIN move_3120 m3120 ON mm."Chassis" = m3120."Chassis"
    LEFT JOIN move_3110 m3110 ON mm."Chassis" = m3110."Chassis"
    LEFT JOIN bill_3120 b3120 ON mm."Chassis" = b3120."Chassis"
    LEFT JOIN po_3120 p3120 ON mm."Chassis" = p3120."Chassis"
    LEFT JOIN gr_3120 g3120 ON mm."Chassis" = g3120."Chassis"
    LEFT JOIN bp_3110 bp3110 ON mm."Chassis" = bp3110."Chassis"
    LEFT JOIN bp_3120 bp3120 ON mm."Chassis" = bp3120."Chassis"
    LEFT JOIN bp_recv recv ON mm."Chassis" = recv."Chassis"
    """

    df = hana_query(sql)
    for c in DETAIL_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[DETAIL_COLUMNS]


# ================= STEP 4: Summary =================

def build_summary(df_mismatch, df_detail, sap_set):
    list_total = len(COUNT_LIST)
    sap_total = len(sap_set)
    mismatch_total = len(df_mismatch)
    only_in_list = int((df_mismatch["Mismatch_Type"] == "Only in List").sum()) if not df_mismatch.empty else 0
    only_in_sap = int((df_mismatch["Mismatch_Type"] == "Only in SAP").sum()) if not df_mismatch.empty else 0

    po_series = pd.Series(dtype="object")
    po_gr_series = pd.Series(dtype="object")
    if not df_detail.empty:
        po_series = df_detail["PO_Number_3120"].dropna().astype(str).str.strip()
        po_series = po_series[po_series != ""]

        po_gr_series = df_detail.loc[df_detail["PO_GR_Date_3120"].notna(), "PO_Number_3120"].dropna().astype(str).str.strip()
        po_gr_series = po_gr_series[po_gr_series != ""]

    po_total = int(po_series.nunique())
    po_gr_done = int(po_gr_series.nunique())
    po_not_gr = po_total - po_gr_done

    return pd.DataFrame([
        ["List_Total", list_total],
        ["SAP_Total", sap_total],
        ["Mismatch_Total", mismatch_total],
        ["Only_in_List", only_in_list],
        ["Only_in_SAP", only_in_sap],
        ["PO_Total_In_System", po_total],
        ["PO_GR_Done", po_gr_done],
        ["PO_Not_GR", po_not_gr],
    ], columns=["Metric", "Value"])


# ================= MAIN =================

def main():
    output_file = "StJames_Audit_Final.xlsx"

    df_mismatch = pd.DataFrame(columns=["Chassis", "Mismatch_Type"])
    df_detail = pd.DataFrame(columns=DETAIL_COLUMNS)
    df_summary = pd.DataFrame(columns=["Metric", "Value"])
    error_message = None
    sap_set = set()

    try:
        sap_set = fetch_true_stock()
        df_mismatch = build_mismatch(sap_set)
        df_detail = fetch_mismatch_details(df_mismatch)
        df_summary = build_summary(df_mismatch, df_detail, sap_set)
    except Exception as exc:
        error_message = str(exc)
        log.exception("Pipeline failed, writing fallback workbook with error info.")
        df_summary = pd.DataFrame([
            ["List_Total", len(COUNT_LIST)],
            ["SAP_Total", 0],
            ["Mismatch_Total", 0],
            ["Only_in_List", 0],
            ["Only_in_SAP", 0],
            ["PO_Total_In_System", 0],
            ["PO_GR_Done", 0],
            ["PO_Not_GR", 0],
            ["Error", error_message],
        ], columns=["Metric", "Value"])
        df_mismatch = pd.DataFrame([["ERROR", error_message]], columns=["Chassis", "Mismatch_Type"])
        df_detail = pd.DataFrame([["ERROR", "Pipeline failed"] + [None] * (len(DETAIL_COLUMNS) - 2)], columns=DETAIL_COLUMNS)

    with pd.ExcelWriter(output_file) as writer:
        df_summary.to_excel(writer, sheet_name="Summary", index=False)
        df_mismatch.to_excel(writer, sheet_name="Mismatch_List", index=False)
        df_detail.to_excel(writer, sheet_name="Mismatch_Detail", index=False)

    log.info("Audit complete. Output file: %s", output_file)


if __name__ == "__main__":
    main()
