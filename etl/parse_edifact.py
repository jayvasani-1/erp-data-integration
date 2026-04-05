import sys
import re
import csv
import pathlib
from datetime import datetime


def parse(text: str):
    # Split EDIFACT segments safely
    segments = [s.strip() for s in re.split(r"'\s*", text) if s.strip()]

    header = {}
    order_lines = []
    current_line = None

    customers = set()
    products = {}

    for seg in segments:
        if seg.startswith("BGM+220+"):
            parts = seg.split("+")
            if len(parts) > 2:
                header["ExternalOrderNo"] = parts[2]

        elif seg.startswith("DTM+137:"):
            val = seg.split(":")[1]
            fmt = "%y%m%d" if len(val) == 6 else "%Y%m%d"
            header["OrderDate"] = datetime.strptime(val, fmt).date().isoformat()

        elif seg.startswith("NAD+BY+"):
            buyer = seg.split("+")[2].split(":")[0]
            header["BuyerCode"] = buyer
            customers.add(buyer)

        elif seg.startswith("NAD+SU+"):
            supplier = seg.split("+")[2].split(":")[0]
            header["SupplierCode"] = supplier
            customers.add(supplier)

        elif seg.startswith("LIN+"):
            parts = seg.split("+")
            line_no = int(parts[1]) if parts[1].isdigit() else len(order_lines) + 1
            sku = parts[3].split(":")[0] if len(parts) > 3 and parts[3] else ""

            current_line = {
                "LineNo": line_no,
                "SKU": sku,
                "Quantity": 0.0,
                "UnitPrice": 0.0,
            }
            order_lines.append(current_line)

            if sku and sku not in products:
                products[sku] = {
                    "SKU": sku,
                    "ProductName": sku,
                    "UoM": "EA",
                    "ListPrice": 0.0,
                }

        elif seg.startswith("QTY+21:") and current_line is not None:
            current_line["Quantity"] = float(seg.split(":")[1])

        elif seg.startswith("PRI+AAA:") and current_line is not None:
            current_line["UnitPrice"] = float(seg.split(":")[1])

    return header, order_lines, customers, products


def main(input_dir, output_dir):
    input_dir = pathlib.Path(input_dir)
    output_dir = pathlib.Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    order_count = 0
    line_count = 0
    all_customers = {}
    all_products = {}

    with open(output_dir / "OrderHeader.csv", "w", newline="", encoding="utf-8") as fh, \
         open(output_dir / "OrderLine.csv", "w", newline="", encoding="utf-8") as fl, \
         open(output_dir / "Customer.csv", "w", newline="", encoding="utf-8") as fc, \
         open(output_dir / "Product.csv", "w", newline="", encoding="utf-8") as fp:

        hw = csv.DictWriter(
            fh,
            fieldnames=["ExternalOrderNo", "OrderDate", "BuyerCode", "SupplierCode", "Currency"],
        )
        lw = csv.DictWriter(
            fl,
            fieldnames=["ExternalOrderNo", "LineNo", "SKU", "Quantity", "UnitPrice"],
        )
        cw = csv.DictWriter(
            fc,
            fieldnames=["CustomerCode", "CustomerName", "City", "Country"],
        )
        pw = csv.DictWriter(
            fp,
            fieldnames=["SKU", "ProductName", "UoM", "ListPrice"],
        )

        hw.writeheader()
        lw.writeheader()
        cw.writeheader()
        pw.writeheader()

        for edi_file in sorted(input_dir.glob("*.edi")):
            header, lines, customers, products = parse(
                edi_file.read_text(encoding="utf-8", errors="ignore")
            )

            if not header.get("ExternalOrderNo"):
                continue

            header["Currency"] = "EUR"
            hw.writerow(header)
            order_count += 1

            for ln in lines:
                ln["ExternalOrderNo"] = header["ExternalOrderNo"]
                lw.writerow(ln)
                line_count += 1

            for c in customers:
                all_customers[c] = {
                    "CustomerCode": c,
                    "CustomerName": c,
                    "City": "",
                    "Country": "",
                }

            all_products.update(products)

        for c in all_customers.values():
            cw.writerow(c)

        for p in all_products.values():
            pw.writerow(p)

    with open(output_dir / "etl_run_log.txt", "a", encoding="utf-8") as log:
        log.write(
            f"[{datetime.utcnow().isoformat()}Z] Parsed {order_count} orders, "
            f"{line_count} lines\n"
        )

    print(f"✅ Parsed {order_count} orders, {line_count} lines → {output_dir}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python etl/parse_edifact.py <input_dir> <output_dir>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
