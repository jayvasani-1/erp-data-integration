import subprocess, sys, os, pathlib
from dotenv import load_dotenv

BASE = pathlib.Path(__file__).resolve().parent
ENV = BASE / ".env"


def sh(cmd, env=None):
    """Run a shell command and echo it."""
    print(">", " ".join(map(str, cmd)))
    try:
        subprocess.check_call(cmd, env=env)
    except subprocess.CalledProcessError as e:
        print(f"❌ Command failed: {e.cmd}")
        sys.exit(e.returncode)


def ensure_env():
    if not ENV.exists():
        src = BASE / ".env.example"
        ENV.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        print("Created .env from .env.example")


def main():
    ensure_env()

    # 0) Install dependencies
    req = BASE / "requirements.txt"
    if req.exists():
        sh([sys.executable, "-m", "pip", "install", "-r", str(req)])

    # Prepare ETL folders
    edi_dir = BASE / "data" / "sample_edifact"
    staging = BASE / "out" / "staging"
    staging.mkdir(parents=True, exist_ok=True)

    # Ensure Python can import local 'etl' modules
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE)

    # 1) Test DB connection
    sh([sys.executable, "-m", "etl.test_connection"], env=env)

    # 2) Setup database (schemas, procs, BI views)
    sh([sys.executable, "-m", "etl.setup_db"], env=env)

    # 3) Parse EDIFACT files -> staging CSVs
    sh([sys.executable, str(BASE / "etl" / "parse_edifact.py"), str(edi_dir), str(staging)])

    # 4) Create empty optional staging CSVs if missing
    (staging / "Customer.csv").write_text("CustomerCode,CustomerName,City,Country\n", encoding="utf-8")
    (staging / "Product.csv").write_text("SKU,ProductName,UoM,ListPrice\n", encoding="utf-8")

    # 5) Load staging -> SQL Server
    sh([sys.executable, "-m", "etl.load_sqlserver", str(staging)], env=env)

    # 6) Open BI files (Power BI / Tableau) if on Windows
    try:
        if os.name == "nt":
            os.startfile(str(BASE / "bi" / "ERP_DEMO.pbids"))
            os.startfile(str(BASE / "bi" / "tableau_connection.tds"))
    except Exception as e:
        print("BI auto-open note:", e)

    print("\n✅ Done. Data loaded into ERP_DEMO. Open Power BI or Tableau from the 'bi' folder if not auto-opened.")


if __name__ == "__main__":
    main()
