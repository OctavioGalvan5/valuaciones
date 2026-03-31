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

    # ---- Tabla expedientes ----
    execute(conn, '''
        CREATE TABLE IF NOT EXISTS expedientes (
            id                SERIAL PRIMARY KEY,
            expediente        TEXT,
            caratula          TEXT,
            creado_por        TEXT,
            activa            INTEGER DEFAULT 1,
            eliminado_por     TEXT,
            fecha_eliminacion TEXT,
            fecha_registro    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # ---- Tabla catastros (id = N° VR) ----
    execute(conn, '''
        CREATE TABLE IF NOT EXISTS catastros (
            id                          SERIAL PRIMARY KEY,
            expediente_id               INTEGER NOT NULL REFERENCES expedientes(id),
            catastro                    TEXT,
            tipo_catastro               TEXT,
            direccion                   TEXT,
            fecha                       TEXT,
            terreno_m2                  REAL DEFAULT 0,
            terreno_frente_lado         TEXT,
            terreno_antes_revision      TEXT,
            usd_m2_terreno              REAL DEFAULT 0,
            productiva_hect             REAL DEFAULT 0,
            productiva_frente_lado      TEXT,
            productiva_antes_revision   TEXT,
            usd_hect_productiva         REAL DEFAULT 0,
            con_monte_hect              REAL DEFAULT 0,
            con_monte_frente_lado       TEXT,
            con_monte_antes_revision    TEXT,
            usd_hect_con_monte          REAL DEFAULT 0,
            cerros_hect                 REAL DEFAULT 0,
            cerros_frente_lado          TEXT,
            cerros_antes_revision       TEXT,
            usd_hect_cerros             REAL DEFAULT 0,
            sup_edif_m2                 REAL DEFAULT 0,
            edif_frente_lado            TEXT,
            edif_antes_revision         TEXT,
            usd_m2_edif                 REAL DEFAULT 0,
            valor_dolar                 REAL DEFAULT 0,
            total_usd_terreno           REAL DEFAULT 0,
            total_usd_edif              REAL DEFAULT 0,
            total_usd                   REAL DEFAULT 0,
            propuesta                   REAL DEFAULT 0,
            monto                       REAL DEFAULT 0,
            denuncia                    TEXT,
            gmaps_zona                  TEXT,
            gmaps_frente                TEXT,
            terreno_total               REAL DEFAULT 0,
            fot                         REAL DEFAULT 0,
            fos                         REAL DEFAULT 0,
            sup_edif_total              REAL DEFAULT 0,
            pisos_maximos               INTEGER DEFAULT 0,
            porcentaje_emprendimiento   REAL DEFAULT 0,
            costo_usd_m2_emprendimiento REAL DEFAULT 0,
            emprendimiento              REAL DEFAULT 0,
            observaciones               TEXT,
            latitud                     REAL,
            longitud                    REAL,
            editado_por                 TEXT,
            fecha_registro              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Columnas nuevas en catastros para bases ya existentes
    for col, definition in [
        ('productiva_frente_lado',    'TEXT'),
        ('productiva_antes_revision', 'TEXT'),
        ('con_monte_frente_lado',     'TEXT'),
        ('con_monte_antes_revision',  'TEXT'),
        ('cerros_frente_lado',        'TEXT'),
        ('cerros_antes_revision',     'TEXT'),
    ]:
        execute(conn, f'ALTER TABLE catastros ADD COLUMN IF NOT EXISTS {col} {definition}')

    # ---- Migración desde valuaciones (si es necesario) ----
    exp_count = fetchscalar(conn, 'SELECT COUNT(*) FROM expedientes')
    if exp_count == 0:
        val_exists = fetchscalar(conn,
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'valuaciones' AND table_schema = 'public'")
        if val_exists:
            # Aseguramos que las columnas nuevas existen en valuaciones antes de migrar
            for col, definition in [
                ('tipo_catastro',               'TEXT'),
                ('monto',                       'REAL DEFAULT 0'),
                ('productiva_hect',             'REAL DEFAULT 0'),
                ('usd_hect_productiva',         'REAL DEFAULT 0'),
                ('con_monte_hect',              'REAL DEFAULT 0'),
                ('usd_hect_con_monte',          'REAL DEFAULT 0'),
                ('cerros_hect',                 'REAL DEFAULT 0'),
                ('usd_hect_cerros',             'REAL DEFAULT 0'),
                ('creado_por',                  'TEXT'),
                ('editado_por',                 'TEXT'),
                ('activa',                      'INTEGER DEFAULT 1'),
                ('eliminado_por',               'TEXT'),
                ('fecha_eliminacion',           'TEXT'),
                ('porcentaje_emprendimiento',   'REAL DEFAULT 0'),
                ('costo_usd_m2_emprendimiento', 'REAL DEFAULT 0'),
                ('emprendimiento',              'REAL DEFAULT 0'),
            ]:
                execute(conn, f'ALTER TABLE valuaciones ADD COLUMN IF NOT EXISTS {col} {definition}')

            # Migrar expedientes (1 por cada valuación)
            execute(conn, '''
                INSERT INTO expedientes (id, expediente, caratula, creado_por,
                    activa, eliminado_por, fecha_eliminacion, fecha_registro)
                SELECT id, expediente, caratula, creado_por,
                    COALESCE(activa, 1), eliminado_por, fecha_eliminacion, fecha_registro
                FROM valuaciones
            ''')

            # Migrar catastros (id = valuacion.id, expediente_id = valuacion.id)
            execute(conn, '''
                INSERT INTO catastros (
                    id, expediente_id,
                    catastro, tipo_catastro, direccion, fecha,
                    terreno_m2, terreno_frente_lado, terreno_antes_revision, usd_m2_terreno,
                    productiva_hect, usd_hect_productiva,
                    con_monte_hect,  usd_hect_con_monte,
                    cerros_hect,     usd_hect_cerros,
                    sup_edif_m2, edif_frente_lado, edif_antes_revision, usd_m2_edif,
                    valor_dolar,
                    total_usd_terreno, total_usd_edif, total_usd, propuesta, monto,
                    denuncia, gmaps_zona, gmaps_frente,
                    terreno_total, fot, fos, sup_edif_total, pisos_maximos,
                    porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
                    observaciones, latitud, longitud, editado_por, fecha_registro
                )
                SELECT
                    id, id,
                    catastro, tipo_catastro, direccion, fecha,
                    COALESCE(terreno_m2, 0), terreno_frente_lado, terreno_antes_revision,
                    COALESCE(usd_m2_terreno, 0),
                    COALESCE(productiva_hect, 0), COALESCE(usd_hect_productiva, 0),
                    COALESCE(con_monte_hect, 0),  COALESCE(usd_hect_con_monte, 0),
                    COALESCE(cerros_hect, 0),     COALESCE(usd_hect_cerros, 0),
                    COALESCE(sup_edif_m2, 0), edif_frente_lado, edif_antes_revision,
                    COALESCE(usd_m2_edif, 0),
                    COALESCE(valor_dolar, 0),
                    COALESCE(total_usd_terreno, 0), COALESCE(total_usd_edif, 0),
                    COALESCE(total_usd, 0), COALESCE(propuesta, 0), COALESCE(monto, 0),
                    denuncia, gmaps_zona, gmaps_frente,
                    COALESCE(terreno_total, 0), COALESCE(fot, 0), COALESCE(fos, 0),
                    COALESCE(sup_edif_total, 0), COALESCE(pisos_maximos, 0),
                    COALESCE(porcentaje_emprendimiento, 0),
                    COALESCE(costo_usd_m2_emprendimiento, 0),
                    COALESCE(emprendimiento, 0),
                    observaciones, latitud, longitud, editado_por, fecha_registro
                FROM valuaciones
            ''')

            # Resetear secuencias
            execute(conn, "SELECT setval(pg_get_serial_sequence('expedientes', 'id'), (SELECT MAX(id) FROM expedientes))")
            execute(conn, "SELECT setval(pg_get_serial_sequence('catastros', 'id'), (SELECT MAX(id) FROM catastros))")

    # ---- Tabla archivos ----
    execute(conn, '''
        CREATE TABLE IF NOT EXISTS archivos (
            id               SERIAL PRIMARY KEY,
            catastro_id      INTEGER NOT NULL,
            nombre_original  TEXT NOT NULL,
            objeto_minio     TEXT NOT NULL,
            tipo             TEXT,
            tamanio          BIGINT DEFAULT 0,
            subido_por       TEXT,
            fecha_subida     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Si la tabla archivos ya existía con valuacion_id, agregar catastro_id y migrar
    execute(conn, 'ALTER TABLE archivos ADD COLUMN IF NOT EXISTS catastro_id INTEGER')
    old_col = fetchscalar(conn,
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name='archivos' AND column_name='valuacion_id'")
    if old_col:
        execute(conn, 'UPDATE archivos SET catastro_id = valuacion_id WHERE catastro_id IS NULL')

    # ---- Tabla usuarios ----
    execute(conn, '''
        CREATE TABLE IF NOT EXISTS usuarios (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
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


def _calcular_catastro(data):
    """Calcula los totales a partir del form data. Devuelve dict con los valores calculados."""
    tipo_catastro  = data.get('tipo_catastro', '').strip()
    sup_edif_m2    = parse_float(data.get('sup_edif_m2'))
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

    if tipo_catastro == 'Rural':
        terreno_m2     = 0
        usd_m2_terreno = 0
        productiva_hect             = parse_float(data.get('productiva_hect'))
        productiva_frente_lado      = data.get('productiva_frente_lado', '').strip()
        productiva_antes_revision   = data.get('productiva_antes_revision', '').strip()
        usd_hect_productiva         = parse_float(data.get('usd_hect_productiva'))
        con_monte_hect              = parse_float(data.get('con_monte_hect'))
        con_monte_frente_lado       = data.get('con_monte_frente_lado', '').strip()
        con_monte_antes_revision    = data.get('con_monte_antes_revision', '').strip()
        usd_hect_con_monte          = parse_float(data.get('usd_hect_con_monte'))
        cerros_hect                 = parse_float(data.get('cerros_hect'))
        cerros_frente_lado          = data.get('cerros_frente_lado', '').strip()
        cerros_antes_revision       = data.get('cerros_antes_revision', '').strip()
        usd_hect_cerros             = parse_float(data.get('usd_hect_cerros'))
        total_usd_terreno = (productiva_hect * usd_hect_productiva +
                             con_monte_hect   * usd_hect_con_monte  +
                             cerros_hect      * usd_hect_cerros)
    else:
        terreno_m2     = parse_float(data.get('terreno_m2'))
        usd_m2_terreno = parse_float(data.get('usd_m2_terreno'))
        productiva_hect = usd_hect_productiva = 0
        productiva_frente_lado = productiva_antes_revision = ''
        con_monte_hect  = usd_hect_con_monte  = 0
        con_monte_frente_lado = con_monte_antes_revision = ''
        cerros_hect     = usd_hect_cerros     = 0
        cerros_frente_lado = cerros_antes_revision = ''
        total_usd_terreno = terreno_m2 * usd_m2_terreno

    total_usd_edif = sup_edif_m2 * usd_m2_edif
    total_usd      = total_usd_terreno + total_usd_edif
    propuesta      = parse_float(data.get('propuesta')) or total_usd * 1.10

    gmaps_zona   = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    lat, lon = extraer_coordenadas(gmaps_frente or gmaps_zona)

    return dict(
        tipo_catastro=tipo_catastro,
        catastro=data.get('catastro', '').strip(),
        direccion=data.get('direccion', '').strip(),
        fecha=data.get('fecha', ''),
        terreno_m2=terreno_m2,
        terreno_frente_lado=data.get('terreno_frente_lado', '').strip(),
        terreno_antes_revision=data.get('terreno_antes_revision', '').strip(),
        usd_m2_terreno=usd_m2_terreno,
        productiva_hect=productiva_hect,
        productiva_frente_lado=productiva_frente_lado,
        productiva_antes_revision=productiva_antes_revision,
        usd_hect_productiva=usd_hect_productiva,
        con_monte_hect=con_monte_hect,
        con_monte_frente_lado=con_monte_frente_lado,
        con_monte_antes_revision=con_monte_antes_revision,
        usd_hect_con_monte=usd_hect_con_monte,
        cerros_hect=cerros_hect,
        cerros_frente_lado=cerros_frente_lado,
        cerros_antes_revision=cerros_antes_revision,
        usd_hect_cerros=usd_hect_cerros,
        sup_edif_m2=sup_edif_m2,
        edif_frente_lado=data.get('edif_frente_lado', '').strip(),
        edif_antes_revision=data.get('edif_antes_revision', '').strip(),
        usd_m2_edif=usd_m2_edif,
        valor_dolar=valor_dolar,
        total_usd_terreno=total_usd_terreno, total_usd_edif=total_usd_edif,
        total_usd=total_usd, propuesta=propuesta,
        monto=parse_float(data.get('monto')),
        denuncia=data.get('denuncia', '').strip(),
        gmaps_zona=gmaps_zona, gmaps_frente=gmaps_frente,
        terreno_total=terreno_total, fot=fot, fos=fos,
        sup_edif_total=sup_edif_total, pisos_maximos=pisos_maximos,
        porcentaje_emprendimiento=porcentaje_emprendimiento,
        costo_usd_m2_emprendimiento=costo_usd_m2_emprendimiento,
        emprendimiento=emprendimiento,
        observaciones=data.get('observaciones', '').strip(),
        latitud=lat, longitud=lon,
    )


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
    estado         = request.args.get('estado', 'activas').strip()
    page           = max(1, int(request.args.get('page', 1) or 1))

    conditions = []
    if estado == 'desactivadas':
        conditions.append('e.activa = 0')
    elif estado == 'todas':
        pass
    else:
        conditions.append('e.activa = 1')

    params = []
    if q:
        conditions.append('''(
            c.catastro ILIKE %s OR e.expediente ILIKE %s OR e.caratula ILIKE %s
            OR c.direccion ILIKE %s OR c.denuncia ILIKE %s
        )''')
        params.extend([f'%{q}%'] * 5)
    if desde:
        conditions.append('c.fecha >= %s')
        params.append(desde)
    if hasta:
        conditions.append('c.fecha <= %s')
        params.append(hasta)
    if filtro_usuario:
        conditions.append('e.creado_por = %s')
        params.append(filtro_usuario)

    where = ' AND '.join(conditions) if conditions else '1=1'

    # Cuando hay filtros sobre catastros, agrupamos por expediente y filtramos
    # los que tienen al menos 1 catastro que cumple
    if q or desde or hasta:
        join_type = 'INNER JOIN'
    else:
        join_type = 'LEFT JOIN'

    conn = get_db()

    count_sql = f'''
        SELECT COUNT(DISTINCT e.id)
        FROM expedientes e
        {join_type} catastros c ON c.expediente_id = e.id
        WHERE {where}
    '''
    total = fetchscalar(conn, count_sql, params)

    list_sql = f'''
        SELECT e.id, e.expediente, e.caratula, e.creado_por, e.activa,
               e.fecha_registro,
               COUNT(c.id) AS num_catastros,
               MIN(c.fecha) AS primera_fecha,
               MAX(c.fecha) AS ultima_fecha,
               STRING_AGG(CAST(c.id AS TEXT), ', ' ORDER BY c.id) AS vr_numeros
        FROM expedientes e
        {join_type} catastros c ON c.expediente_id = e.id
        WHERE {where}
        GROUP BY e.id, e.expediente, e.caratula, e.creado_por, e.activa, e.fecha_registro
        ORDER BY e.id DESC
        LIMIT %s OFFSET %s
    '''
    expedientes = fetchall(conn, list_sql, params + [PER_PAGE, (page - 1) * PER_PAGE])
    usuarios_db = fetchall(conn, 'SELECT username FROM usuarios ORDER BY username')
    conn.close()

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    return render_template('index.html',
                           expedientes=expedientes,
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'],
                           q=q, desde=desde, hasta=hasta,
                           filtro_usuario=filtro_usuario,
                           estado=estado,
                           page=page, total_pages=total_pages, total=total,
                           usuarios_lista=[u['username'] for u in usuarios_db])


@app.route('/expediente/<int:exp_id>')
@login_required
def expediente_detail(exp_id):
    conn = get_db()
    exp = fetchone(conn, 'SELECT * FROM expedientes WHERE id = %s', (exp_id,))
    if not exp:
        conn.close()
        return redirect(url_for('index'))
    catastros = fetchall(conn,
        'SELECT * FROM catastros WHERE expediente_id = %s ORDER BY id ASC', (exp_id,))
    conn.close()
    return render_template('expediente.html',
                           exp=exp,
                           catastros=catastros,
                           usuario=session['usuario'])


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
        sql = '''
            SELECT c.id, e.expediente, e.caratula, c.direccion
            FROM catastros c
            JOIN expedientes e ON e.id = c.expediente_id
            WHERE c.catastro = %s AND e.activa = 1
        '''
        params = [catastro]
        if exclude_id:
            sql += ' AND c.id != %s'
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
        sql = '''
            SELECT c.id, c.catastro, e.expediente, e.caratula, c.direccion, c.latitud, c.longitud
            FROM catastros c
            JOIN expedientes e ON e.id = c.expediente_id
            WHERE c.latitud IS NOT NULL AND c.longitud IS NOT NULL AND e.activa = 1
        '''
        params = []
        if exclude_id:
            sql += ' AND c.id != %s'
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


@app.route('/nueva')
@login_required
def nueva():
    return render_template('form.html',
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'])


@app.route('/agregar', methods=['POST'])
@login_required
def agregar():
    """Crea un nuevo expediente con su primer catastro."""
    usuario_actual = session['usuario']
    data = request.form

    conn = get_db()

    # Crear expediente
    exp_id = fetchscalar(conn, '''
        INSERT INTO expedientes (expediente, caratula, creado_por, activa)
        VALUES (%s, %s, %s, 1) RETURNING id
    ''', (
        data.get('expediente', '').strip(),
        data.get('caratula', '').strip(),
        usuario_actual,
    ))

    # Crear catastro
    c = _calcular_catastro(data)
    cat_id = fetchscalar(conn, '''
        INSERT INTO catastros (
            expediente_id,
            catastro, tipo_catastro, direccion, fecha,
            terreno_m2, terreno_frente_lado, terreno_antes_revision, usd_m2_terreno,
            productiva_hect, productiva_frente_lado, productiva_antes_revision, usd_hect_productiva,
            con_monte_hect,  con_monte_frente_lado,  con_monte_antes_revision,  usd_hect_con_monte,
            cerros_hect,     cerros_frente_lado,     cerros_antes_revision,     usd_hect_cerros,
            sup_edif_m2, edif_frente_lado, edif_antes_revision, usd_m2_edif,
            valor_dolar,
            total_usd_terreno, total_usd_edif, total_usd, propuesta, monto,
            denuncia, gmaps_zona, gmaps_frente,
            terreno_total, fot, fos, sup_edif_total, pisos_maximos,
            porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
            observaciones, latitud, longitud
        ) VALUES (
            %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        ) RETURNING id
    ''', (
        exp_id,
        c['catastro'], c['tipo_catastro'], c['direccion'], c['fecha'],
        c['terreno_m2'], c['terreno_frente_lado'], c['terreno_antes_revision'], c['usd_m2_terreno'],
        c['productiva_hect'], c['productiva_frente_lado'], c['productiva_antes_revision'], c['usd_hect_productiva'],
        c['con_monte_hect'],  c['con_monte_frente_lado'],  c['con_monte_antes_revision'],  c['usd_hect_con_monte'],
        c['cerros_hect'],     c['cerros_frente_lado'],     c['cerros_antes_revision'],     c['usd_hect_cerros'],
        c['sup_edif_m2'], c['edif_frente_lado'], c['edif_antes_revision'], c['usd_m2_edif'],
        c['valor_dolar'],
        c['total_usd_terreno'], c['total_usd_edif'], c['total_usd'], c['propuesta'], c['monto'],
        c['denuncia'], c['gmaps_zona'], c['gmaps_frente'],
        c['terreno_total'], c['fot'], c['fos'], c['sup_edif_total'], c['pisos_maximos'],
        c['porcentaje_emprendimiento'], c['costo_usd_m2_emprendimiento'], c['emprendimiento'],
        c['observaciones'], c['latitud'], c['longitud'],
    ))

    # Archivos adjuntos opcionales
    for file in request.files.getlist('archivos'):
        if not file or not file.filename:
            continue
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            continue
        objeto_minio = f'{cat_id}/{uuid.uuid4().hex}{ext}'
        file_data = file.read()
        tamanio = len(file_data)
        try:
            minio_client.put_object(MINIO_BUCKET, objeto_minio, BytesIO(file_data), tamanio,
                                    content_type=file.content_type or 'application/octet-stream')
            execute(conn, '''
                INSERT INTO archivos (catastro_id, nombre_original, objeto_minio, tipo, tamanio, subido_por)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (cat_id, file.filename, objeto_minio, ext, tamanio, usuario_actual))
        except S3Error as e:
            print(f'[MinIO] Error al subir archivo: {e}')

    conn.commit()
    conn.close()
    return redirect(url_for('expediente_detail', exp_id=exp_id))


@app.route('/expediente/<int:exp_id>/nuevo_catastro')
@login_required
def nuevo_catastro(exp_id):
    conn = get_db()
    exp = fetchone(conn, 'SELECT * FROM expedientes WHERE id = %s', (exp_id,))
    conn.close()
    if not exp:
        return redirect(url_for('index'))
    return render_template('form.html',
                           expediente_obj=exp,
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'])


@app.route('/expediente/<int:exp_id>/guardar_catastro', methods=['POST'])
@login_required
def guardar_catastro(exp_id):
    """Agrega un catastro a un expediente existente."""
    usuario_actual = session['usuario']
    data = request.form

    conn = get_db()
    exp = fetchone(conn, 'SELECT id FROM expedientes WHERE id = %s', (exp_id,))
    if not exp:
        conn.close()
        return redirect(url_for('index'))

    c = _calcular_catastro(data)
    cat_id = fetchscalar(conn, '''
        INSERT INTO catastros (
            expediente_id,
            catastro, tipo_catastro, direccion, fecha,
            terreno_m2, terreno_frente_lado, terreno_antes_revision, usd_m2_terreno,
            productiva_hect, productiva_frente_lado, productiva_antes_revision, usd_hect_productiva,
            con_monte_hect,  con_monte_frente_lado,  con_monte_antes_revision,  usd_hect_con_monte,
            cerros_hect,     cerros_frente_lado,     cerros_antes_revision,     usd_hect_cerros,
            sup_edif_m2, edif_frente_lado, edif_antes_revision, usd_m2_edif,
            valor_dolar,
            total_usd_terreno, total_usd_edif, total_usd, propuesta, monto,
            denuncia, gmaps_zona, gmaps_frente,
            terreno_total, fot, fos, sup_edif_total, pisos_maximos,
            porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
            observaciones, latitud, longitud
        ) VALUES (
            %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s
        ) RETURNING id
    ''', (
        exp_id,
        c['catastro'], c['tipo_catastro'], c['direccion'], c['fecha'],
        c['terreno_m2'], c['terreno_frente_lado'], c['terreno_antes_revision'], c['usd_m2_terreno'],
        c['productiva_hect'], c['productiva_frente_lado'], c['productiva_antes_revision'], c['usd_hect_productiva'],
        c['con_monte_hect'],  c['con_monte_frente_lado'],  c['con_monte_antes_revision'],  c['usd_hect_con_monte'],
        c['cerros_hect'],     c['cerros_frente_lado'],     c['cerros_antes_revision'],     c['usd_hect_cerros'],
        c['sup_edif_m2'], c['edif_frente_lado'], c['edif_antes_revision'], c['usd_m2_edif'],
        c['valor_dolar'],
        c['total_usd_terreno'], c['total_usd_edif'], c['total_usd'], c['propuesta'], c['monto'],
        c['denuncia'], c['gmaps_zona'], c['gmaps_frente'],
        c['terreno_total'], c['fot'], c['fos'], c['sup_edif_total'], c['pisos_maximos'],
        c['porcentaje_emprendimiento'], c['costo_usd_m2_emprendimiento'], c['emprendimiento'],
        c['observaciones'], c['latitud'], c['longitud'],
    ))

    for file in request.files.getlist('archivos'):
        if not file or not file.filename:
            continue
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in ALLOWED_EXTENSIONS:
            continue
        objeto_minio = f'{cat_id}/{uuid.uuid4().hex}{ext}'
        file_data = file.read()
        tamanio = len(file_data)
        try:
            minio_client.put_object(MINIO_BUCKET, objeto_minio, BytesIO(file_data), tamanio,
                                    content_type=file.content_type or 'application/octet-stream')
            execute(conn, '''
                INSERT INTO archivos (catastro_id, nombre_original, objeto_minio, tipo, tamanio, subido_por)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (cat_id, file.filename, objeto_minio, ext, tamanio, usuario_actual))
        except S3Error as e:
            print(f'[MinIO] Error al subir archivo: {e}')

    conn.commit()
    conn.close()
    return redirect(url_for('expediente_detail', exp_id=exp_id))


@app.route('/desactivar/<int:exp_id>', methods=['POST'])
@login_required
def desactivar(exp_id):
    conn = get_db()
    execute(conn,
        'UPDATE expedientes SET activa = 0, eliminado_por = %s, fecha_eliminacion = %s WHERE id = %s',
        (session['usuario'], datetime.now().strftime('%Y-%m-%d %H:%M'), exp_id))
    conn.commit()
    conn.close()
    return redirect(request.referrer or url_for('index'))


@app.route('/reactivar/<int:exp_id>', methods=['POST'])
@login_required
def reactivar(exp_id):
    conn = get_db()
    execute(conn,
        'UPDATE expedientes SET activa = 1, eliminado_por = NULL, fecha_eliminacion = NULL WHERE id = %s',
        (exp_id,))
    conn.commit()
    conn.close()
    return redirect(request.referrer or url_for('index'))


@app.route('/ver/<int:cat_id>')
@login_required
def ver(cat_id):
    conn = get_db()
    catastro = fetchone(conn, 'SELECT * FROM catastros WHERE id = %s', (cat_id,))
    if not catastro:
        conn.close()
        return redirect(url_for('index'))
    exp = fetchone(conn, 'SELECT * FROM expedientes WHERE id = %s', (catastro['expediente_id'],))
    archivos = fetchall(conn, 'SELECT * FROM archivos WHERE catastro_id = %s ORDER BY fecha_subida DESC', (cat_id,))
    conn.close()
    return render_template('form.html',
                           viendo=catastro,
                           expediente_obj=exp,
                           archivos=archivos,
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'])


@app.route('/editar/<int:cat_id>')
@login_required
def editar(cat_id):
    conn = get_db()
    catastro = fetchone(conn, 'SELECT * FROM catastros WHERE id = %s', (cat_id,))
    if not catastro:
        conn.close()
        return redirect(url_for('index'))
    exp = fetchone(conn, 'SELECT * FROM expedientes WHERE id = %s', (catastro['expediente_id'],))
    archivos = fetchall(conn, 'SELECT * FROM archivos WHERE catastro_id = %s ORDER BY fecha_subida DESC', (cat_id,))
    conn.close()
    return render_template('form.html',
                           editando=catastro,
                           expediente_obj=exp,
                           archivos=archivos,
                           today=datetime.now().strftime('%Y-%m-%d'),
                           usuario=session['usuario'])


@app.route('/actualizar/<int:cat_id>', methods=['POST'])
@login_required
def actualizar(cat_id):
    usuario_actual = session['usuario']
    data = request.form

    conn = get_db()
    catastro = fetchone(conn, 'SELECT expediente_id FROM catastros WHERE id = %s', (cat_id,))
    if not catastro:
        conn.close()
        return redirect(url_for('index'))
    exp_id = catastro['expediente_id']

    # Actualizar expediente si se enviaron esos campos
    new_expediente = data.get('expediente', '').strip()
    new_caratula   = data.get('caratula', '').strip()
    if new_expediente or new_caratula:
        execute(conn, '''
            UPDATE expedientes SET
                expediente = COALESCE(NULLIF(%s,''), expediente),
                caratula   = COALESCE(NULLIF(%s,''), caratula)
            WHERE id = %s
        ''', (new_expediente, new_caratula, exp_id))

    c = _calcular_catastro(data)
    execute(conn, '''
        UPDATE catastros SET
            catastro=%s, tipo_catastro=%s, direccion=%s, fecha=%s,
            terreno_m2=%s, terreno_frente_lado=%s, terreno_antes_revision=%s, usd_m2_terreno=%s,
            productiva_hect=%s, productiva_frente_lado=%s, productiva_antes_revision=%s, usd_hect_productiva=%s,
            con_monte_hect=%s,  con_monte_frente_lado=%s,  con_monte_antes_revision=%s,  usd_hect_con_monte=%s,
            cerros_hect=%s,     cerros_frente_lado=%s,     cerros_antes_revision=%s,     usd_hect_cerros=%s,
            sup_edif_m2=%s, edif_frente_lado=%s, edif_antes_revision=%s, usd_m2_edif=%s,
            valor_dolar=%s,
            total_usd_terreno=%s, total_usd_edif=%s, total_usd=%s, propuesta=%s, monto=%s,
            denuncia=%s, gmaps_zona=%s, gmaps_frente=%s,
            terreno_total=%s, fot=%s, fos=%s, sup_edif_total=%s, pisos_maximos=%s,
            porcentaje_emprendimiento=%s, costo_usd_m2_emprendimiento=%s, emprendimiento=%s,
            observaciones=%s, latitud=%s, longitud=%s,
            editado_por=%s
        WHERE id=%s
    ''', (
        c['catastro'], c['tipo_catastro'], c['direccion'], c['fecha'],
        c['terreno_m2'], c['terreno_frente_lado'], c['terreno_antes_revision'], c['usd_m2_terreno'],
        c['productiva_hect'], c['productiva_frente_lado'], c['productiva_antes_revision'], c['usd_hect_productiva'],
        c['con_monte_hect'],  c['con_monte_frente_lado'],  c['con_monte_antes_revision'],  c['usd_hect_con_monte'],
        c['cerros_hect'],     c['cerros_frente_lado'],     c['cerros_antes_revision'],     c['usd_hect_cerros'],
        c['sup_edif_m2'], c['edif_frente_lado'], c['edif_antes_revision'], c['usd_m2_edif'],
        c['valor_dolar'],
        c['total_usd_terreno'], c['total_usd_edif'], c['total_usd'], c['propuesta'], c['monto'],
        c['denuncia'], c['gmaps_zona'], c['gmaps_frente'],
        c['terreno_total'], c['fot'], c['fos'], c['sup_edif_total'], c['pisos_maximos'],
        c['porcentaje_emprendimiento'], c['costo_usd_m2_emprendimiento'], c['emprendimiento'],
        c['observaciones'], c['latitud'], c['longitud'],
        usuario_actual,
        cat_id,
    ))
    conn.commit()
    conn.close()
    return redirect(url_for('expediente_detail', exp_id=exp_id))


# ---- Archivos (MinIO) ----

@app.route('/subir_archivo/<int:catastro_id>', methods=['POST'])
@login_required
def subir_archivo(catastro_id):
    conn = get_db()
    cat = fetchone(conn, 'SELECT expediente_id FROM catastros WHERE id = %s', (catastro_id,))
    if not cat:
        conn.close()
        return redirect(url_for('index'))

    if 'archivo' not in request.files:
        conn.close()
        return redirect(url_for('editar', cat_id=catastro_id))

    file = request.files['archivo']
    if not file.filename:
        conn.close()
        return redirect(url_for('editar', cat_id=catastro_id))

    nombre_original = file.filename
    ext = os.path.splitext(nombre_original)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        conn.close()
        return redirect(url_for('editar', cat_id=catastro_id))

    objeto_minio = f'{catastro_id}/{uuid.uuid4().hex}{ext}'
    data = file.read()
    tamanio = len(data)

    try:
        minio_client.put_object(
            MINIO_BUCKET, objeto_minio, BytesIO(data), tamanio,
            content_type=file.content_type or 'application/octet-stream',
        )
    except S3Error as e:
        print(f'[MinIO] Error al subir archivo: {e}')
        conn.close()
        return redirect(url_for('editar', cat_id=catastro_id))

    execute(conn, '''
        INSERT INTO archivos (catastro_id, nombre_original, objeto_minio, tipo, tamanio, subido_por)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (catastro_id, nombre_original, objeto_minio, ext, tamanio, session['usuario']))
    conn.commit()
    conn.close()
    return redirect(url_for('editar', cat_id=catastro_id))


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
    return redirect(url_for('editar', cat_id=archivo['catastro_id']))


@app.route('/exportar_excel')
@login_required
def exportar_excel():
    from openpyxl import Workbook
    from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    q              = request.args.get('q', '').strip()
    desde          = request.args.get('desde', '').strip()
    hasta          = request.args.get('hasta', '').strip()
    filtro_usuario = request.args.get('usuario', '').strip()

    conditions = ['e.activa = 1']
    params = []
    if q:
        conditions.append('(c.catastro ILIKE %s OR e.expediente ILIKE %s OR e.caratula ILIKE %s OR c.direccion ILIKE %s OR c.denuncia ILIKE %s)')
        params.extend([f'%{q}%'] * 5)
    if desde:
        conditions.append('c.fecha >= %s')
        params.append(desde)
    if hasta:
        conditions.append('c.fecha <= %s')
        params.append(hasta)
    if filtro_usuario:
        conditions.append('e.creado_por = %s')
        params.append(filtro_usuario)

    conn = get_db()
    rows = fetchall(conn, f'''
        SELECT c.id, e.expediente, c.catastro, c.tipo_catastro, e.caratula,
               c.direccion, c.fecha, c.denuncia,
               c.terreno_m2, c.sup_edif_m2, c.usd_m2_terreno, c.usd_m2_edif,
               c.valor_dolar, c.total_usd_terreno, c.total_usd_edif, c.total_usd,
               c.propuesta, c.fot, c.fos, c.sup_edif_total, c.pisos_maximos,
               c.emprendimiento, e.creado_por, c.fecha_registro, c.observaciones
        FROM catastros c
        JOIN expedientes e ON e.id = c.expediente_id
        WHERE {" AND ".join(conditions)}
        ORDER BY c.id DESC
    ''', params)
    conn.close()

    wb = Workbook()
    ws = wb.active
    ws.title = 'Valuaciones'

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

    ws.row_dimensions[1].height = 36
    for col_idx, (titulo, _, ancho, _align) in enumerate(COLUMNAS, 1):
        cell = ws.cell(row=1, column=col_idx, value=titulo)
        cell.fill      = header_fill
        cell.font      = header_font
        cell.alignment = header_align
        cell.border    = cell_border
        ws.column_dimensions[get_column_letter(col_idx)].width = ancho

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

    ws.freeze_panes = 'A2'
    ws.auto_filter.ref = f'A1:{get_column_letter(len(COLUMNAS))}1'

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
