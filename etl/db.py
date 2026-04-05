import os
from dotenv import load_dotenv
import pytds

load_dotenv()


def connect(db: str | None = None, autocommit: bool = True):
    try:
        return pytds.connect(
            server=os.getenv("SQLSERVER_SERVER", "localhost"),
            database=db or os.getenv("SQLSERVER_DATABASE", "ERP_DEMO"),
            user=os.getenv("SQLSERVER_UID", "sa"),
            password=os.getenv("SQLSERVER_PWD", "yourpassword"),
            port=int(os.getenv("SQLSERVER_PORT", "number")),
            autocommit=autocommit,
        )
    except Exception as e:
        raise RuntimeError(
            "❌ Failed to connect to SQL Server.\n"
            "Check:\n"
            "- SQL Server is running\n"
            "- Login / password\n"
            "- TCP/IP enabled (1433)\n"
            "- .env configuration\n\n"
            f"Original error:\n{e}"
        )
