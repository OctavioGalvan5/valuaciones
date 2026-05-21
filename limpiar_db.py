import app

print("Conectando a la base de datos...")
conn = app.get_db()

print("Borrando todos los expedientes, catastros y archivos...")
app.execute(conn, "TRUNCATE TABLE archivos, catastros, expedientes RESTART IDENTITY CASCADE;")
conn.commit()
conn.close()

print("¡Base de datos limpia! (Los usuarios se mantuvieron intactos)")
