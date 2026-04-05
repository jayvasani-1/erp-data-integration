from etl.db import connect

if __name__ == "__main__":
    cn = connect(autocommit=True)
    cur = cn.cursor()

    cur.execute("SELECT @@VERSION")
    row = cur.fetchone()

    print("Connected to SQL Server OK")
    print(row[0])

    cur.close()
    # DO NOT explicitly cn.close() when using autocommit + pypyodbc
