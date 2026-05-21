import app
print("Dropping old valuaciones table...")
conn = app.get_db()
app.execute(conn, "DROP TABLE IF EXISTS valuaciones CASCADE;")
conn.commit()
conn.close()
print("Done.")
