import psycopg2
conn = psycopg2.connect("postgresql://postgres:rmdrzs2et6gezhuf@62.72.11.137:5433/postgres")
cur = conn.cursor()
cur.execute("SELECT datname FROM pg_database ORDER BY datname;")
for row in cur.fetchall():
    print(row[0])
conn.close()
