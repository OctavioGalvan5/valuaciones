import app

print("Conectando a la base de datos...")
conn = app.get_db()

print("Borrando todos los expedientes, catastros, automotores, archivos y contadores...")
try:
    # Truncar las tablas relacionales y reiniciar secuencias
    app.execute(conn, "TRUNCATE TABLE archivos, catastros, expedientes, automotores, contadores_vr, contadores_recuento RESTART IDENTITY CASCADE;")
    conn.commit()
    print("¡Base de datos PostgreSQL limpia con éxito!")
except Exception as e:
    conn.rollback()
    print(f"Error al limpiar la base de datos: {e}")
finally:
    conn.close()

print("Borrando todos los archivos adjuntos en el almacenamiento MinIO...")
try:
    # Listar y eliminar de manera recursiva todos los objetos en el bucket de MinIO
    objetos = app.minio_client.list_objects(app.MINIO_BUCKET, recursive=True)
    eliminados = 0
    for obj in objetos:
        app.minio_client.remove_object(app.MINIO_BUCKET, obj.object_name)
        eliminados += 1
    print(f"¡Almacenamiento MinIO limpio con éxito! ({eliminados} objetos eliminados)")
except Exception as e:
    print(f"Error al limpiar MinIO: {e}")

print("\n¡Limpieza completada! (Los usuarios y credenciales se mantuvieron intactos)")
