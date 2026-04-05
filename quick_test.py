from dotenv import load_dotenv
import os, pypyodbc as o

# Load .env values (make sure your .env is in this folder)
load_dotenv()

# Build connection string
cs = (
    f"Driver={{{os.getenv('DB_DRIVER')}}};"
    f"Server={os.getenv('DB_SERVER')};"
    f"Database={os.getenv('DB_NAME')};"
    f"Uid={os.getenv('DB_USER')};Pwd={os.getenv('DB_PASSWORD')};"
    f"Encrypt={os.getenv('DB_ENCRYPT','no')};"
    f"TrustServerCertificate={os.getenv('DB_TRUST_CERT','yes')};"
)

print("\nConnection string:\n", cs)

# Test connection
try:
    with o.connect(cs, timeout=5) as cn:
        cur = cn.cursor()
        cur.execute("SELECT @@SERVERNAME, DB_NAME()")
        print("\n✅ SUCCESS:", cur.fetchall())
except Exception as e:
    print("\n❌ FAILED:", e)
