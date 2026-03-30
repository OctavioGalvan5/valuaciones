import os
import re
import math
import uuid
from io import BytesIO
from datetime import datetime
from functools import wraps

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, redirect, url_for, jsonify, session, abort, Response, stream_with_context
from minio import Minio
from minio.error import S3Error
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'valuaciones-sarmiento-2024-xk9')
app.config['MAX_CONTENT_LENGTH'] = 20 * 1024 * 1024  # 20 MB

DATABASE_URL  = os.environ.get('DATABASE_URL', '')
MINIO_ENDPOINT   = os.environ.get('MINIO_ENDPOINT',   '62.72.11.137:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', 'kzkvmhwlhrjbebbt')
MINIO_BUCKET     = os.environ.get('MINIO_BUCKET',     'valuaciones')
MINIO_SECURE     = os.environ.get('MINIO_SECURE', 'false').lower() == 'true'

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE,
)

USUARIOS_INICIALES = [
    ('admin',   'Sarmiento302'),
    ('Mariano', 'Sarmiento302'),
    ('Luis',    'Sarmiento302'),
    ('Octavio', 'Sarmiento302'),
]

ALLOWED_EXTENSIONS = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.jpg', '.jpeg', '.png'}
PER_PAGE = 20


# ---- DB helpers ----

def get_db():
    return psycopg2.connect(DATABASE_URL)


def fetchall(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or [])
        return cur.fetchall()


def fetchone(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or [])
        return cur.fetchone()


def fetchscalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        row = cur.fetchone()
        return row[0] if row else None


def execute(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or [])


# ---- MinIO ----

def init_minio():
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
    except S3Error as e:
        print(f'[MinIO] Error al inicializar bucket: {e}')


# ---- DB init ----

def init_db():
    conn = get_db()
    execute(conn, '''
        CREATE TABLE IF NOT EXISTS valuaciones (
            id                          SERIAL PRIMARY KEY,
            expediente                  TEXT,
            caratula                    TEXT,
            catastro                    TEXT,
            direccion                   TEXT,
            fecha                       TEXT,
            terreno_m2                  REAL DEFAULT 0,
            terreno_frente_lado         TEXT,
            terreno_antes_revision      TEXT,
            usd_m2_terreno              REAL DEFAULT 0,
            sup_edif_m2                 REAL DEFAULT 0,
            edif_frente_lado            TEXT,
            edif_antes_revision         TEXT,
            usd_m2_edif                 REAL DEFAULT 0,
            valor_dolar                 REAL DEFAULT 0,
            total_usd_terreno           REAL DEFAULT 0,
            total_usd_edif              REAL DEFAULT 0,
            total_usd                   REAL DEFAULT 0,
            propuesta                   REAL DEFAULT 0,
            denuncia                    TEXT,
            gmaps_zona                  TEXT,
            gmaps_frente                TEXT,
            terreno_total               REAL DEFAULT 0,
            fot                         REAL DEFAULT 0,
            fos                         REAL DEFAULT 0,
            sup_edif_total              REAL DEFAULT 0,
            pisos_maximos               INTEGER DEFAULT 0,
            observaciones               TEXT,
            latitud                     REAL,
            longitud                    REAL,
            fecha_registro              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            porcentaje_emprendimiento   REAL DEFAULT 0,
            costo_usd_m2_emprendimiento REAL DEFAULT 0,
            emprendimiento              REAL DEFAULT 0,
            creado_por                  TEXT,
            editado_por                 TEXT,
            activa                      INTEGER DEFAULT 1,
            eliminado_por               TEXT,
            fecha_eliminacion           TEXT
        )
    ''')

    # Columnas nuevas para bases ya existentes
    for col, definition in [
        ('porcentaje_emprendimiento',   'REAL DEFAULT 0'),
        ('costo_usd_m2_emprendimiento', 'REAL DEFAULT 0'),
        ('emprendimiento',              'REAL DEFAULT 0'),
        ('creado_por',                  'TEXT'),
        ('editado_por',                 'TEXT'),
        ('activa',                      'INTEGER DEFAULT 1'),
        ('eliminado_por',               'TEXT'),
        ('fecha_eliminacion',           'TEXT'),
        ('tipo_catastro',               'TEXT'),
        ('monto',                       'REAL DEFAULT 0'),
    ]:
        execute(conn, f'ALTER TABLE valuaciones ADD COLUMN IF NOT EXISTS {col} {definition}')

    execute(conn, '''
        CREATE TABLE IF NOT EXISTS usuarios (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    ''')

    execute(conn, '''
        CREATE TABLE IF NOT EXISTS archivos (
            id             SERIAL PRIMARY KEY,
            valuacion_id   INTEGER NOT NULL REFERENCES valuaciones(id),
            nombre_original TEXT NOT NULL,
            objeto_minio   TEXT NOT NULL,
            tipo           TEXT,
            tamanio        BIGINT DEFAULT 0,
            subido_por     TEXT,
            fecha_subida   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    for username, password in USUARIOS_INICIALES:
        if not fetchone(conn, 'SELECT id FROM usuarios WHERE username = %s', (username,)):
            execute(conn, 'INSERT INTO usuarios (username, password_hash) VALUES (%s, %s)',
                    (username, generate_password_hash(password)))

    conn.commit()
    conn.close()
    init_minio()


# ---- Auth ----

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'usuario' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


@app.template_filter('filesize')
def filesize_filter(size):
    if not size:
        return '—'
    if size < 1024:
        return f'{size} B'
    elif size < 1024 * 1024:
        return f'{size / 1024:.1f} KB'
    return f'{size / 1024 / 1024:.1f} MB'


# ---- Helpers ----

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))


def extraer_coordenadas(url):
    if not url:
        return None, None
    lat_matches = re.findall(r'!3d(-?[\d.]+)', url)
    lon_matches = re.findall(r'!4d(-?[\d.]+)', url)
    if lat_matches and lon_matches:
        return float(lat_matches[-1]), float(lon_matches[-1])
    for pattern in [r'q=(-?\d+\.?\d*),(-?\d+\.?\d*)', r'll=(-?\d+\.?\d*),(-?\d+\.?\d*)', r'@(-?\d+\.?\d*),(-?\d+\.?\d*)']:
        m = re.search(pattern, url)
        if m:
            return float(m.group(1)), float(m.group(2))
    return None, None


def parse_float(val):
    try:
        return float(val) if val else 0
    except (ValueError, TypeError):
        return 0


def parse_int(val):
    try:
        return int(val) if val else 0
    except (ValueError, TypeError):
        return 0


def base_context():
    conn = get_db()
    total = fetchscalar(conn, 'SELECT COUNT(*) FROM valuaciones WHERE activa = 1')
    valuaciones = fetchall(conn,
        'SELECT * FROM valuaciones WHERE activa = 1 ORDER BY id DESC LIMIT %s OFFSET %s',
        [PER_PAGE, 0])
    usuarios_db = fetchall(conn, 'SELECT username FROM usuarios ORDER BY username')
    conn.close()
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    return {
        'valuaciones':    valuaciones,
        'today':          datetime.now().strftime('%Y-%m-%d'),
        'usuario':        session['usuario'],
        'q': '', 'desde': '', 'hasta': '', 'filtro_usuario': '',
        'page': 1, 'total_pages': total_pages, 'total': total,
        'usuarios_lista': [u['username'] for u in usuarios_db],
        'archivos':       [],
    }


# ---- Routes ----

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'usuario' in session:
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        conn = get_db()
        user = fetchone(conn, 'SELECT * FROM usuarios WHERE username = %s', (username,))
        conn.close()
        if user and check_password_hash(user['password_hash'], password):
            session['usuario'] = username
            return redirect(url_for('index'))
        error = 'Usuario o contraseña incorrectos.'
    return render_template('login.html', error=error)


@app.route('/logout')
def logout():
    session.pop('usuario', None)
    return redirect(url_for('login'))


@app.route('/')
@login_required
def index():
    q              = request.args.get('q', '').strip()
    desde          = request.args.get('desde', '').strip()
    hasta          = request.args.get('hasta', '').strip()
    filtro_usuario = request.args.get('usuario', '').strip()
    page           = max(1, int(request.args.get('page', 1) or 1))

    conditions = ['activa = 1']
    params = []

    if q:
        conditions.append('(catastro ILIKE %s OR expediente ILIKE %s OR caratula ILIKE %s OR direccion ILIKE %s OR denuncia ILIKE %s)')
        params.extend([f'%{q}%'] * 5)
    if desde:
        conditions.append('fecha >= %s')
        params.append(desde)
    if hasta:
        conditions.append('fecha <= %s')
        params.append(hasta)
    if filtro_usuario:
        conditions.append('creado_por = %s')
        params.append(filtro_usuario)

    where = ' AND '.join(conditions)
    conn = get_db()
    total = fetchscalar(conn, f'SELECT COUNT(*) FROM valuaciones WHERE {where}', params)
    valuaciones = fetchall(conn,
        f'SELECT * FROM valuaciones WHERE {where} ORDER BY id DESC LIMIT %s OFFSET %s',
        params + [PER_PAGE, (page - 1) * PER_PAGE])
    usuarios_db = fetchall(conn, 'SELECT username FROM usuarios ORDER BY username')
    conn.close()

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    return render_template('index.html',
                           valuaciones=valuaciones,
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'],
                           q=q, desde=desde, hasta=hasta,
                           filtro_usuario=filtro_usuario,
                           page=page, total_pages=total_pages, total=total,
                           usuarios_lista=[u['username'] for u in usuarios_db],
                           archivos=[])


@app.route('/verificar_catastro', methods=['POST'])
@login_required
def verificar_catastro():
    data = request.get_json()
    catastro    = data.get('catastro', '').strip()
    gmaps_zona  = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    exclude_id  = data.get('exclude_id')

    alertas = []
    conn = get_db()

    if catastro:
        sql = 'SELECT id, expediente, caratula, direccion FROM valuaciones WHERE catastro = %s AND activa = 1'
        params = [catastro]
        if exclude_id:
            sql += ' AND id != %s'
            params.append(exclude_id)
        existente = fetchone(conn, sql, params)
        if existente:
            alertas.append({
                'tipo': 'duplicado',
                'mensaje': f'El catastro {catastro} ya fue tasado (VR #{existente["id"]}) - '
                           f'Expediente: {existente["expediente"]}, '
                           f'Carátula: {existente["caratula"]}, '
                           f'Dirección: {existente["direccion"]}'
            })

    gmaps_link = gmaps_frente or gmaps_zona
    lat, lon = extraer_coordenadas(gmaps_link)

    if lat is not None and lon is not None:
        sql = 'SELECT id, catastro, expediente, caratula, direccion, latitud, longitud FROM valuaciones WHERE latitud IS NOT NULL AND longitud IS NOT NULL AND activa = 1'
        params = []
        if exclude_id:
            sql += ' AND id != %s'
            params.append(exclude_id)
        registros = fetchall(conn, sql, params)
        for reg in registros:
            dist = haversine(lat, lon, reg['latitud'], reg['longitud'])
            if dist < 1.0:
                dist_metros = round(dist * 1000)
                alertas.append({
                    'tipo': 'proximidad',
                    'mensaje': f'El catastro {reg["catastro"]} (VR #{reg["id"]}) está a {dist_metros}m - '
                               f'Expediente: {reg["expediente"]}, '
                               f'Carátula: {reg["caratula"]}, '
                               f'Dirección: {reg["direccion"]}',
                    'distancia': dist_metros
                })

    conn.close()
    return jsonify({'alertas': alertas, 'coordenadas': {'lat': lat, 'lon': lon}})


@app.route('/agregar', methods=['POST'])
@login_required
def agregar():
    data = request.form
    usuario_actual = session['usuario']

    terreno_m2  = parse_float(data.get('terreno_m2'))
    sup_edif_m2 = parse_float(data.get('sup_edif_m2'))
    usd_m2_terreno = parse_float(data.get('usd_m2_terreno'))
    usd_m2_edif    = parse_float(data.get('usd_m2_edif'))
    terreno_total  = parse_float(data.get('terreno_total'))
    fot            = parse_float(data.get('fot'))
    fos            = parse_float(data.get('fos'))
    pisos_maximos  = parse_int(data.get('pisos_maximos'))
    valor_dolar    = parse_float(data.get('valor_dolar'))
    sup_edif_total = parse_float(data.get('sup_edif_total_calc'))
    porcentaje_emprendimiento   = parse_float(data.get('porcentaje_emprendimiento'))
    costo_usd_m2_emprendimiento = parse_float(data.get('costo_usd_m2_emprendimiento'))
    emprendimiento = parse_float(data.get('emprendimiento'))

    total_usd_terreno = terreno_m2 * usd_m2_terreno
    total_usd_edif    = sup_edif_m2 * usd_m2_edif
    total_usd         = total_usd_terreno + total_usd_edif
    propuesta         = parse_float(data.get('propuesta')) or total_usd * 1.10

    gmaps_zona   = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    lat, lon = extraer_coordenadas(gmaps_frente or gmaps_zona)

    conn = get_db()
    new_id = fetchscalar(conn, '''
        INSERT INTO valuaciones (
            expediente, caratula, catastro, tipo_catastro, direccion, fecha,
            terreno_m2, terreno_frente_lado, terreno_antes_revision, usd_m2_terreno,
            sup_edif_m2, edif_frente_lado, edif_antes_revision, usd_m2_edif,
            valor_dolar,
            total_usd_terreno, total_usd_edif, total_usd, propuesta,
            denuncia,
            gmaps_zona, gmaps_frente,
            terreno_total, fot, fos, sup_edif_total, pisos_maximos,
            porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
            observaciones, latitud, longitud,
            creado_por, activa, monto
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s,
            %s,
            %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        ) RETURNING id
    ''', (
        data.get('expediente', '').strip(),
        data.get('caratula', '').strip(),
        data.get('catastro', '').strip(),
        data.get('tipo_catastro', '').strip(),
        data.get('direccion', '').strip(),
        data.get('fecha', ''),
        terreno_m2, data.get('terreno_frente_lado', '').strip(),
        data.get('terreno_antes_revision', '').strip(), usd_m2_terreno,
        sup_edif_m2, data.get('edif_frente_lado', '').strip(),
        data.get('edif_antes_revision', '').strip(), usd_m2_edif,
        valor_dolar,
        total_usd_terreno, total_usd_edif, total_usd, propuesta,
        data.get('denuncia', '').strip(),
        gmaps_zona, gmaps_frente,
        terreno_total, fot, fos, sup_edif_total, pisos_maximos,
        porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
        data.get('observaciones', '').strip(),
        lat, lon,
        usuario_actual, 1, parse_float(data.get('monto')),
    ))
    # Procesar archivos adjuntos opcionales
    for file in request.files.getlist('archivos'):
        if not file or not file.filename:
            continue
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            continue
        objeto_minio = f'{new_id}/{uuid.uuid4().hex}{ext}'
        file_data = file.read()
        tamanio = len(file_data)
        try:
            minio_client.put_object(MINIO_BUCKET, objeto_minio, BytesIO(file_data), tamanio,
                                    content_type=file.content_type or 'application/octet-stream')
            execute(conn, '''
                INSERT INTO archivos (valuacion_id, nombre_original, objeto_minio, tipo, tamanio, subido_por)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (new_id, file.filename, objeto_minio, ext, tamanio, usuario_actual))
        except S3Error as e:
            print(f'[MinIO] Error al subir archivo: {e}')

    conn.commit()
    conn.close()
    return redirect(url_for('index'))


@app.route('/desactivar/<int:id>', methods=['POST'])
@login_required
def desactivar(id):
    conn = get_db()
    execute(conn,
        'UPDATE valuaciones SET activa = 0, eliminado_por = %s, fecha_eliminacion = %s WHERE id = %s',
        (session['usuario'], datetime.now().strftime('%Y-%m-%d %H:%M'), id))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


@app.route('/ver/<int:id>')
@login_required
def ver(id):
    conn = get_db()
    valuacion = fetchone(conn, 'SELECT * FROM valuaciones WHERE id = %s', (id,))
    archivos  = fetchall(conn, 'SELECT * FROM archivos WHERE valuacion_id = %s ORDER BY fecha_subida DESC', (id,))
    conn.close()
    if valuacion is None:
        return redirect(url_for('index'))
    ctx = base_context()
    ctx['viendo']  = valuacion
    ctx['archivos'] = archivos
    return render_template('index.html', **ctx)


@app.route('/editar/<int:id>')
@login_required
def editar(id):
    conn = get_db()
    valuacion = fetchone(conn, 'SELECT * FROM valuaciones WHERE id = %s', (id,))
    archivos  = fetchall(conn, 'SELECT * FROM archivos WHERE valuacion_id = %s ORDER BY fecha_subida DESC', (id,))
    conn.close()
    if valuacion is None:
        return redirect(url_for('index'))
    ctx = base_context()
    ctx['editando'] = valuacion
    ctx['archivos'] = archivos
    return render_template('index.html', **ctx)


@app.route('/actualizar/<int:id>', methods=['POST'])
@login_required
def actualizar(id):
    data = request.form
    usuario_actual = session['usuario']

    terreno_m2  = parse_float(data.get('terreno_m2'))
    sup_edif_m2 = parse_float(data.get('sup_edif_m2'))
    usd_m2_terreno = parse_float(data.get('usd_m2_terreno'))
    usd_m2_edif    = parse_float(data.get('usd_m2_edif'))
    terreno_total  = parse_float(data.get('terreno_total'))
    fot            = parse_float(data.get('fot'))
    fos            = parse_float(data.get('fos'))
    pisos_maximos  = parse_int(data.get('pisos_maximos'))
    valor_dolar    = parse_float(data.get('valor_dolar'))
    sup_edif_total = parse_float(data.get('sup_edif_total_calc'))
    porcentaje_emprendimiento   = parse_float(data.get('porcentaje_emprendimiento'))
    costo_usd_m2_emprendimiento = parse_float(data.get('costo_usd_m2_emprendimiento'))
    emprendimiento = parse_float(data.get('emprendimiento'))

    total_usd_terreno = terreno_m2 * usd_m2_terreno
    total_usd_edif    = sup_edif_m2 * usd_m2_edif
    total_usd         = total_usd_terreno + total_usd_edif
    propuesta         = parse_float(data.get('propuesta')) or total_usd * 1.10

    gmaps_zona   = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    lat, lon = extraer_coordenadas(gmaps_frente or gmaps_zona)

    conn = get_db()
    execute(conn, '''
        UPDATE valuaciones SET
            expediente=%s, caratula=%s, catastro=%s, tipo_catastro=%s, direccion=%s, fecha=%s,
            terreno_m2=%s, terreno_frente_lado=%s, terreno_antes_revision=%s, usd_m2_terreno=%s,
            sup_edif_m2=%s, edif_frente_lado=%s, edif_antes_revision=%s, usd_m2_edif=%s,
            valor_dolar=%s,
            total_usd_terreno=%s, total_usd_edif=%s, total_usd=%s, propuesta=%s,
            denuncia=%s,
            gmaps_zona=%s, gmaps_frente=%s,
            terreno_total=%s, fot=%s, fos=%s, sup_edif_total=%s, pisos_maximos=%s,
            porcentaje_emprendimiento=%s, costo_usd_m2_emprendimiento=%s, emprendimiento=%s,
            observaciones=%s, latitud=%s, longitud=%s,
            editado_por=%s, monto=%s
        WHERE id=%s
    ''', (
        data.get('expediente', '').strip(),
        data.get('caratula', '').strip(),
        data.get('catastro', '').strip(),
        data.get('tipo_catastro', '').strip(),
        data.get('direccion', '').strip(),
        data.get('fecha', ''),
        terreno_m2, data.get('terreno_frente_lado', '').strip(),
        data.get('terreno_antes_revision', '').strip(), usd_m2_terreno,
        sup_edif_m2, data.get('edif_frente_lado', '').strip(),
        data.get('edif_antes_revision', '').strip(), usd_m2_edif,
        valor_dolar,
        total_usd_terreno, total_usd_edif, total_usd, propuesta,
        data.get('denuncia', '').strip(),
        gmaps_zona, gmaps_frente,
        terreno_total, fot, fos, sup_edif_total, pisos_maximos,
        porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
        data.get('observaciones', '').strip(),
        lat, lon,
        usuario_actual, parse_float(data.get('monto')),
        id,
    ))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


# ---- Archivos (MinIO) ----

@app.route('/subir_archivo/<int:valuacion_id>', methods=['POST'])
@login_required
def subir_archivo(valuacion_id):
    if 'archivo' not in request.files:
        return redirect(url_for('editar', id=valuacion_id))

    file = request.files['archivo']
    if not file.filename:
        return redirect(url_for('editar', id=valuacion_id))

    nombre_original = file.filename
    ext = os.path.splitext(nombre_original)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return redirect(url_for('editar', id=valuacion_id))

    objeto_minio = f'{valuacion_id}/{uuid.uuid4().hex}{ext}'
    data = file.read()
    tamanio = len(data)

    try:
        minio_client.put_object(
            MINIO_BUCKET,
            objeto_minio,
            BytesIO(data),
            tamanio,
            content_type=file.content_type or 'application/octet-stream',
        )
    except S3Error as e:
        print(f'[MinIO] Error al subir archivo: {e}')
        return redirect(url_for('editar', id=valuacion_id))

    conn = get_db()
    execute(conn, '''
        INSERT INTO archivos (valuacion_id, nombre_original, objeto_minio, tipo, tamanio, subido_por)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (valuacion_id, nombre_original, objeto_minio, ext, tamanio, session['usuario']))
    conn.commit()
    conn.close()
    return redirect(url_for('editar', id=valuacion_id))


@app.route('/archivo/<int:id>')
@login_required
def descargar_archivo(id):
    conn = get_db()
    archivo = fetchone(conn, 'SELECT * FROM archivos WHERE id = %s', (id,))
    conn.close()
    if not archivo:
        abort(404)
    try:
        obj = minio_client.get_object(MINIO_BUCKET, archivo['objeto_minio'])
        ext = (archivo['tipo'] or '').lower()
        disposition = 'inline' if ext in ('.jpg', '.jpeg', '.png', '.pdf') else f'attachment; filename="{archivo["nombre_original"]}"'
        content_type = obj.headers.get('content-type', 'application/octet-stream')
        return Response(
            stream_with_context(obj.stream(32 * 1024)),
            content_type=content_type,
            headers={'Content-Disposition': disposition},
        )
    except S3Error as e:
        print(f'[MinIO] Error al servir archivo: {e}')
        abort(500)


@app.route('/eliminar_archivo/<int:id>', methods=['POST'])
@login_required
def eliminar_archivo(id):
    conn = get_db()
    archivo = fetchone(conn, 'SELECT * FROM archivos WHERE id = %s', (id,))
    if not archivo:
        conn.close()
        return redirect(url_for('index'))
    try:
        minio_client.remove_object(MINIO_BUCKET, archivo['objeto_minio'])
    except S3Error as e:
        print(f'[MinIO] Error al eliminar objeto: {e}')
    execute(conn, 'DELETE FROM archivos WHERE id = %s', (id,))
    conn.commit()
    conn.close()
    return redirect(url_for('editar', id=archivo['valuacion_id']))


@app.route('/exportar_excel')
@login_required
def exportar_excel():
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side, numbers
    from openpyxl.utils import get_column_letter

    # Respetar filtros activos
    q              = request.args.get('q', '').strip()
    desde          = request.args.get('desde', '').strip()
    hasta          = request.args.get('hasta', '').strip()
    filtro_usuario = request.args.get('usuario', '').strip()

    conditions = ['activa = 1']
    params = []
    if q:
        conditions.append('(catastro ILIKE %s OR expediente ILIKE %s OR caratula ILIKE %s OR direccion ILIKE %s OR denuncia ILIKE %s)')
        params.extend([f'%{q}%'] * 5)
    if desde:
        conditions.append('fecha >= %s')
        params.append(desde)
    if hasta:
        conditions.append('fecha <= %s')
        params.append(hasta)
    if filtro_usuario:
        conditions.append('creado_por = %s')
        params.append(filtro_usuario)

    conn = get_db()
    rows = fetchall(conn, f'SELECT * FROM valuaciones WHERE {" AND ".join(conditions)} ORDER BY id DESC', params)
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.title = 'Valuaciones'

    # Estilos
    header_fill = PatternFill('solid', fgColor='1E3A8A')
    header_font = Font(bold=True, color='DBEAFE', size=10)
    header_align = Alignment(horizontal='center', vertical='center', wrap_text=True)
    alt_fill = PatternFill('solid', fgColor='F8F9FB')
    border_side = Side(style='thin', color='D8DCE3')
    cell_border = Border(left=border_side, right=border_side, top=border_side, bottom=border_side)
    center = Alignment(horizontal='center', vertical='center')
    right_align = Alignment(horizontal='right', vertical='center')
    usd_fmt = '#,##0.00'

    COLUMNAS = [
        ('VR #',            'id',                           10,  'center'),
        ('Expediente',      'expediente',                   16,  'left'),
        ('Catastro',        'catastro',                     18,  'left'),
        ('Tipo Catastro',   'tipo_catastro',                14,  'center'),
        ('Carátula',        'caratula',                     28,  'left'),
        ('Dirección',       'direccion',                    32,  'left'),
        ('Fecha',           'fecha',                        13,  'center'),
        ('Tipo',            'denuncia',                     13,  'center'),
        ('Terreno m²',      'terreno_m2',                   13,  'right'),
        ('Sup. Edif. m²',   'sup_edif_m2',                  13,  'right'),
        ('U$S/m² Terreno',  'usd_m2_terreno',               15,  'right'),
        ('U$S/m² Edif.',    'usd_m2_edif',                  13,  'right'),
        ('Valor Dólar',     'valor_dolar',                  13,  'right'),
        ('Total U$S Terr.', 'total_usd_terreno',            15,  'right'),
        ('Total U$S Edif.', 'total_usd_edif',               15,  'right'),
        ('Total U$S',       'total_usd',                    14,  'right'),
        ('Propuesta U$S',   'propuesta',                    15,  'right'),
        ('FOT',             'fot',                          8,   'center'),
        ('FOS',             'fos',                          8,   'center'),
        ('Sup. Edif. Total','sup_edif_total',               15,  'right'),
        ('Pisos Máx.',      'pisos_maximos',                10,  'center'),
        ('Emprendimiento',  'emprendimiento',               16,  'right'),
        ('Creado por',      'creado_por',                   14,  'center'),
        ('Fecha Registro',  'fecha_registro',               18,  'center'),
        ('Observaciones',   'observaciones',                40,  'left'),
    ]

    USD_COLS = {'usd_m2_terreno','usd_m2_edif','valor_dolar','total_usd_terreno',
                'total_usd_edif','total_usd','propuesta','emprendimiento','terreno_m2','sup_edif_m2','sup_edif_total'}

    # Encabezados
    ws.row_dimensions[1].height = 36
    for col_idx, (titulo, _, ancho, _align) in enumerate(COLUMNAS, 1):
        cell = ws.cell(row=1, column=col_idx, value=titulo)
        cell.fill   = header_fill
        cell.font   = header_font
        cell.alignment = header_align
        cell.border = cell_border
        ws.column_dimensions[get_column_letter(col_idx)].width = ancho

    # Datos
    for row_idx, v in enumerate(rows, 2):
        fill = alt_fill if row_idx % 2 == 0 else PatternFill('solid', fgColor='FFFFFF')
        for col_idx, (_, campo, _, align) in enumerate(COLUMNAS, 1):
            valor = v[campo] if campo in v.keys() else None
            if isinstance(valor, datetime):
                valor = valor.strftime('%d/%m/%Y %H:%M')
            cell = ws.cell(row=row_idx, column=col_idx, value=valor)
            cell.fill   = fill
            cell.border = cell_border
            cell.font   = Font(size=9)
            if align == 'center':
                cell.alignment = center
            elif align == 'right':
                cell.alignment = right_align
            else:
                cell.alignment = Alignment(vertical='center', wrap_text=(campo == 'observaciones'))
            if campo in USD_COLS and valor is not None:
                cell.number_format = usd_fmt

    # Freeze y autofilter
    ws.freeze_panes = 'A2'
    ws.auto_filter.ref = f'A1:{get_column_letter(len(COLUMNAS))}1'

    # Guardar en buffer
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)

    fecha_hoy = datetime.now().strftime('%Y%m%d')
    return Response(
        buf,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename="valuaciones_{fecha_hoy}.xlsx"'},
    )


init_db()

if __name__ == '__main__':
    app.run(debug=True, port=5002)
