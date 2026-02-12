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


def build_summary(sap_set, df_mismatch, df_detail):
    po_series = df_detail.get("PO_Number_3120", pd.Series(dtype="object"))
    po_gr_series = df_detail.get("PO_GR_Date", pd.Series(dtype="object"))

    po_total = int(po_series.dropna().nunique()) if not po_series.empty else 0
    po_gr_count = int(
        df_detail.loc[
            df_detail["PO_Number_3120"].notna() & df_detail["PO_GR_Date"].notna(),
            "PO_Number_3120",
        ].nunique()
    ) if not df_detail.empty else 0

    return pd.DataFrame(
        [
            ["List_Total", len(set(COUNT_LIST))],
            ["SAP_Total", len(set(sap_set))],
            ["Mismatch_Total", len(df_mismatch)],
            ["Only_in_List", int((df_mismatch["Mismatch_Type"] == "Only in List").sum())],
            ["Only_in_SAP", int((df_mismatch["Mismatch_Type"] == "Only in SAP").sum())],
            ["PO_Total_In_System", po_total],
            ["PO_GR_Done", po_gr_count],
            ["PO_Not_GR", po_total - po_gr_count],
        ],
        columns=["Metric", "Count"],
    )


# ================= STEP 3A: 统计 (3120 only) =================

def fetch_statistics(serial_list):

    if not serial_list:
        return pd.DataFrame(
            columns=[
                "Chassis",
                "SalesOrder_Count",
                "PGI_Count",
                "Reverse_Count",
                "Last_Movement_Date",
                "Last_Movement_Type",
            ]
        )

    in_list = "(" + ",".join(f"'{c}'" for c in serial_list) + ")"

    sql = f"""
    SELECT
        obj."SERNR" AS "Chassis",
        COUNT(DISTINCT vbak."VBELN") AS "SalesOrder_Count",
        SUM(CASE WHEN mseg."BWART"='601' THEN 1 ELSE 0 END) AS "PGI_Count",
        SUM(CASE WHEN mseg."BWART"='602' THEN 1 ELSE 0 END) AS "Reverse_Count",
        MAX(mseg."BUDAT_MKPF") AS "Last_Movement_Date",
        MAX(mseg."BWART") AS "Last_Movement_Type"

    FROM "SAPHANADB"."OBJK" obj
    LEFT JOIN "SAPHANADB"."SER02" s
        ON obj."OBKNR" = s."OBKNR"
    LEFT JOIN "SAPHANADB"."VBAK" vbak
        ON s."SDAUFNR" = vbak."VBELN"
       AND vbak."VKORG" = '3120'
    LEFT JOIN "SAPHANADB"."NSDM_V_MSEG" mseg
        ON mseg."KDAUF" = vbak."VBELN"

    WHERE obj."SERNR" IN {in_list}

    GROUP BY obj."SERNR"
    """

    return hana_query(sql)


def fetch_mismatch_details(serial_list):

    if not serial_list:
        return pd.DataFrame(
            columns=[
                "Chassis",
                "Mismatch_Type",
                "SalesOrder_3120",
                "SalesOrder_3110",
                "SalesOrderPGI_Doc",
                "PGI_Date",
                "Is_Reverse_PGI",
                "Is_Last_Movement_PGI",
                "Invoice_No",
                "PO_Number_3120",
                "PO_Number_Count",
                "PO_GR_Date",
                "BillTo_3110",
            ]
        )

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
    so3120 AS (
        SELECT DISTINCT
            b."Chassis",
            v."VBELN" AS "SalesOrder_3120",
            COALESCE(vbkd."BSTKD", v."BSTNK") AS "PO_Number_3120"
        FROM base b
        JOIN "SAPHANADB"."VBAK" v
            ON b."SalesOrder" = v."VBELN"
           AND v."VKORG" = '3120'
        LEFT JOIN "SAPHANADB"."VBKD" vbkd
            ON vbkd."VBELN" = v."VBELN"
           AND vbkd."POSNR" = '000000'
    ),
    so3110 AS (
        SELECT DISTINCT
            b."Chassis",
            v."VBELN" AS "SalesOrder_3110",
            bp."KUNNR" AS "BillTo_3110"
        FROM base b
        JOIN "SAPHANADB"."VBAK" v
            ON b."SalesOrder" = v."VBELN"
           AND v."VKORG" = '3110'
        LEFT JOIN "SAPHANADB"."VBPA" bp
            ON bp."VBELN" = v."VBELN"
           AND bp."PARVW" = 'RE'
    ),
    move_agg AS (
        SELECT
            s."Chassis",
            MAX(CASE WHEN m."BWART" = '601' THEN m."MBLNR" END) AS "SalesOrderPGI_Doc",
            MAX(CASE WHEN m."BWART" = '601' THEN m."BUDAT_MKPF" END) AS "PGI_Date",
            MAX(CASE WHEN m."BWART" = '101' THEN m."BUDAT_MKPF" END) AS "PO_GR_Date",
            CASE WHEN SUM(CASE WHEN m."BWART" = '602' THEN 1 ELSE 0 END) > 0 THEN 'Y' ELSE 'N' END AS "Is_Reverse_PGI",
            CASE
                WHEN MAX(CASE WHEN m."BWART" IN ('601','602') THEN m."BUDAT_MKPF" END)
                   = MAX(CASE WHEN m."BWART" = '601' THEN m."BUDAT_MKPF" END)
                THEN 'Y' ELSE 'N'
            END AS "Is_Last_Movement_PGI"
        FROM so3120 s
        LEFT JOIN "SAPHANADB"."NSDM_V_MSEG" m
            ON m."KDAUF" = s."SalesOrder_3120"
        GROUP BY s."Chassis"
    ),
    inv_agg AS (
        SELECT
            s."Chassis",
            MAX(vbrk."VBELN") AS "Invoice_No"
        FROM so3120 s
        LEFT JOIN "SAPHANADB"."VBRP" vbrp
            ON vbrp."AUBEL" = s."SalesOrder_3120"
        LEFT JOIN "SAPHANADB"."VBRK" vbrk
            ON vbrk."VBELN" = vbrp."VBELN"
        GROUP BY s."Chassis"
    )
    SELECT
        b."Chassis",
        MAX(s3120."SalesOrder_3120") AS "SalesOrder_3120",
        MAX(s3110."SalesOrder_3110") AS "SalesOrder_3110",
        MAX(mv."SalesOrderPGI_Doc") AS "SalesOrderPGI_Doc",
        MAX(mv."PGI_Date") AS "PGI_Date",
        MAX(mv."Is_Reverse_PGI") AS "Is_Reverse_PGI",
        MAX(mv."Is_Last_Movement_PGI") AS "Is_Last_Movement_PGI",
        MAX(inv."Invoice_No") AS "Invoice_No",
        MAX(s3120."PO_Number_3120") AS "PO_Number_3120",
        COUNT(DISTINCT s3120."PO_Number_3120") AS "PO_Number_Count",
        MAX(mv."PO_GR_Date") AS "PO_GR_Date",
        MAX(s3110."BillTo_3110") AS "BillTo_3110"
    FROM base b
    LEFT JOIN so3120 s3120
        ON b."Chassis" = s3120."Chassis"
    LEFT JOIN so3110 s3110
        ON b."Chassis" = s3110."Chassis"
    LEFT JOIN move_agg mv
        ON b."Chassis" = mv."Chassis"
    LEFT JOIN inv_agg inv
        ON b."Chassis" = inv."Chassis"
    GROUP BY b."Chassis"
    """

    return hana_query(sql)


# ================= MAIN =================

def main():

    sap_set = fetch_true_stock()

    df_mismatch = build_mismatch(sap_set)
    mismatch_list = df_mismatch["Chassis"].tolist()

    df_stats = fetch_statistics(mismatch_list)
    df_detail = fetch_mismatch_details(mismatch_list)
    df_summary = build_summary(sap_set, df_mismatch, df_detail)

    df_stats = df_stats.merge(df_mismatch, on="Chassis", how="left")
    df_detail = df_detail.merge(df_mismatch, on="Chassis", how="left")

    if "Mismatch_Type_x" in df_detail.columns:
        df_detail["Mismatch_Type"] = df_detail["Mismatch_Type_y"].combine_first(
            df_detail["Mismatch_Type_x"]
        )
        df_detail = df_detail.drop(columns=["Mismatch_Type_x", "Mismatch_Type_y"])

    detail_cols = [
        "Chassis",
        "Mismatch_Type",
        "SalesOrder_3120",
        "SalesOrder_3110",
        "SalesOrderPGI_Doc",
        "PGI_Date",
        "Is_Reverse_PGI",
        "Is_Last_Movement_PGI",
        "Invoice_No",
        "PO_Number_3120",
        "PO_Number_Count",
        "PO_GR_Date",
        "BillTo_3110",
    ]
    df_detail = df_detail.reindex(columns=detail_cols)

    with pd.ExcelWriter("StJames_Audit_Final.xlsx") as writer:
        df_summary.to_excel(writer, sheet_name="Summary", index=False)
        df_mismatch.to_excel(writer, sheet_name="Mismatch_List", index=False)
        df_detail.to_excel(writer, sheet_name="Mismatch_Detail", index=False)
        df_stats.to_excel(writer, sheet_name="Mismatch_Statistics", index=False)

    log.info("Audit complete.")


if __name__ == "__main__":
    main()
