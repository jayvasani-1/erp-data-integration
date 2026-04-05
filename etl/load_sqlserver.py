import os
import sys
import csv
import pathlib
from dotenv import load_dotenv
from etl.db import connect

load_dotenv()


def truncate_tables(cn):
    """Truncate staging tables if they exist."""
    cur = cn.cursor()
    staging_tables = [
        "stg.Customer",
        "stg.Product",
        "stg.OrderHeader",
        "stg.OrderLine",
    ]

    for table in staging_tables:
        cur.execute(
            f"""
            IF OBJECT_ID('{table}', 'U') IS NOT NULL
                TRUNCATE TABLE {table}
            """
        )

    cur.close()


def bulk_insert(cn, table, file_path):
    """Bulk insert CSV rows into a SQL Server table using parameterized queries."""
    path = pathlib.Path(file_path)
    if not path.exists():
        print(f"⚠️ File not found: {path}")
        return 0

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        if not columns:
            return 0

        # Escape column names safely
        columns_escaped = [f"[{c}]" for c in columns]

        rows = [
            tuple(row[c] if row[c] != "" else None for c in columns)
            for row in reader
        ]

        if not rows:
            return 0

        placeholders = ",".join("?" for _ in columns)
        sql = f"""
        INSERT INTO {table} ({",".join(columns_escaped)})
        VALUES ({placeholders})
        """

        cur = cn.cursor()
        try:
            # IMPORTANT for SQL Server performance
            cur.fast_executemany = True

            cur.executemany(sql, rows)
            cn.commit()
            return len(rows)

        except Exception as e:
            cn.rollback()
            print(f"⚠️ Failed inserting into {table}: {e}")
            return 0

        finally:
            cur.close()


def exec_if_exists(cn, proc_name):
    """Execute a stored procedure only if it exists."""
    cur = cn.cursor()
    cur.execute(
        f"""
        IF OBJECT_ID('{proc_name}', 'P') IS NOT NULL
        BEGIN
            SET NOCOUNT ON;
            EXEC {proc_name};
        END
        """
    )
    cur.close()


def main(staging_dir):
    db = os.getenv("SQLSERVER_DATABASE", "ERP_DEMO")

    with connect(db, autocommit=False) as cn:
        # 1️⃣ Truncate staging tables
        truncate_tables(cn)

        # 2️⃣ Load CSV files
        staging_path = pathlib.Path(staging_dir)
        counts = {}

        table_file_map = [
            ("stg.OrderHeader", "OrderHeader.csv"),
            ("stg.OrderLine", "OrderLine.csv"),
            ("stg.Customer", "Customer.csv"),
            ("stg.Product", "Product.csv"),
        ]

        for table, filename in table_file_map:
            file_path = staging_path / filename
            counts[table] = bulk_insert(cn, table, file_path)

        # 3️⃣ Execute transformation procedures
        procedures = [
            "core.usp_stage_orders_from_flat",
            "core.usp_load_orders",
        ]

        for proc in procedures:
            try:
                exec_if_exists(cn, proc)
            except Exception as e:
                print(f"⚠️ Failed executing stored procedure {proc}: {e}")

        print("✅ Loaded:", counts)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl/load_sqlserver.py <staging_dir>")
        sys.exit(1)

    main(sys.argv[1])
