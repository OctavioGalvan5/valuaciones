from flask import Flask, render_template, request, redirect, url_for, jsonify, session
import sqlite3
import re
import math
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps

app = Flask(__name__)
app.secret_key = 'valuaciones-sarmiento-2024-xk9'
DATABASE = 'valuaciones.db'

USUARIOS_INICIALES = [
    ('admin',   'Sarmiento302'),
    ('Mariano', 'Sarmiento302'),
    ('Luis',    'Sarmiento302'),
]


def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute('''
        CREATE TABLE IF NOT EXISTS valuaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente TEXT,
            caratula TEXT,
            catastro TEXT,
            direccion TEXT,
            fecha TEXT,
            terreno_m2 REAL DEFAULT 0,
            terreno_frente_lado TEXT,
            terreno_antes_revision TEXT,
            usd_m2_terreno REAL DEFAULT 0,
            sup_edif_m2 REAL DEFAULT 0,
            edif_frente_lado TEXT,
            edif_antes_revision TEXT,
            usd_m2_edif REAL DEFAULT 0,
            valor_dolar REAL DEFAULT 0,
            total_usd_terreno REAL DEFAULT 0,
            total_usd_edif REAL DEFAULT 0,
            total_usd REAL DEFAULT 0,
            propuesta REAL DEFAULT 0,
            denuncia TEXT,
            gmaps_zona TEXT,
            gmaps_frente TEXT,
            terreno_total REAL DEFAULT 0,
            fot REAL DEFAULT 0,
            fos REAL DEFAULT 0,
            sup_edif_total REAL DEFAULT 0,
            pisos_maximos INTEGER DEFAULT 0,
            observaciones TEXT,
            latitud REAL,
            longitud REAL,
            fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # Columnas nuevas para bases de datos existentes
    for col, definition in [
        ('porcentaje_emprendimiento',   'REAL DEFAULT 0'),
        ('costo_usd_m2_emprendimiento', 'REAL DEFAULT 0'),
        ('emprendimiento',              'REAL DEFAULT 0'),
        ('creado_por',                  'TEXT'),
        ('editado_por',                 'TEXT'),
        ('activa',                      'INTEGER DEFAULT 1'),
        ('eliminado_por',               'TEXT'),
        ('fecha_eliminacion',           'TEXT'),
    ]:
        try:
            conn.execute(f'ALTER TABLE valuaciones ADD COLUMN {col} {definition}')
        except Exception:
            pass

    conn.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    ''')
    for username, password in USUARIOS_INICIALES:
        existing = conn.execute('SELECT id FROM usuarios WHERE username = ?', (username,)).fetchone()
        if not existing:
            conn.execute(
                'INSERT INTO usuarios (username, password_hash) VALUES (?, ?)',
                (username, generate_password_hash(password))
            )
    conn.commit()
    conn.close()


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'usuario' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


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
    match = re.search(r'q=(-?\d+\.?\d*),(-?\d+\.?\d*)', url)
    if match:
        return float(match.group(1)), float(match.group(2))
    match = re.search(r'll=(-?\d+\.?\d*),(-?\d+\.?\d*)', url)
    if match:
        return float(match.group(1)), float(match.group(2))
    match = re.search(r'@(-?\d+\.?\d*),(-?\d+\.?\d*)', url)
    if match:
        return float(match.group(1)), float(match.group(2))
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


# ---- Auth ----

@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'usuario' in session:
        return redirect(url_for('index'))
    error = None
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        conn = get_db()
        user = conn.execute('SELECT * FROM usuarios WHERE username = ?', (username,)).fetchone()
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


# ---- Main routes ----

PER_PAGE = 20

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
        conditions.append('(catastro LIKE ? OR expediente LIKE ? OR caratula LIKE ? OR direccion LIKE ? OR denuncia LIKE ?)')
        params.extend([f'%{q}%'] * 5)
    if desde:
        conditions.append('fecha >= ?')
        params.append(desde)
    if hasta:
        conditions.append('fecha <= ?')
        params.append(hasta)
    if filtro_usuario:
        conditions.append('creado_por = ?')
        params.append(filtro_usuario)

    where = ' AND '.join(conditions)

    conn = get_db()
    total = conn.execute(f'SELECT COUNT(*) FROM valuaciones WHERE {where}', params).fetchone()[0]
    valuaciones = conn.execute(
        f'SELECT * FROM valuaciones WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?',
        params + [PER_PAGE, (page - 1) * PER_PAGE]
    ).fetchall()
    usuarios_db = conn.execute('SELECT username FROM usuarios ORDER BY username').fetchall()
    conn.close()

    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template('index.html',
                           valuaciones=valuaciones, today=today,
                           usuario=session['usuario'],
                           q=q, desde=desde, hasta=hasta,
                           filtro_usuario=filtro_usuario,
                           page=page, total_pages=total_pages, total=total,
                           usuarios_lista=[u['username'] for u in usuarios_db])


@app.route('/verificar_catastro', methods=['POST'])
@login_required
def verificar_catastro():
    data = request.get_json()
    catastro = data.get('catastro', '').strip()
    gmaps_zona = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    exclude_id = data.get('exclude_id')

    alertas = []
    conn = get_db()

    if catastro:
        query = 'SELECT id, expediente, caratula, direccion FROM valuaciones WHERE catastro = ? AND activa = 1'
        params = [catastro]
        if exclude_id:
            query += ' AND id != ?'
            params.append(exclude_id)
        existente = conn.execute(query, params).fetchone()
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
        query = 'SELECT id, catastro, expediente, caratula, direccion, latitud, longitud FROM valuaciones WHERE latitud IS NOT NULL AND longitud IS NOT NULL AND activa = 1'
        params = []
        if exclude_id:
            query += ' AND id != ?'
            params.append(exclude_id)
        registros = conn.execute(query, params).fetchall()
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

    terreno_m2 = parse_float(data.get('terreno_m2'))
    sup_edif_m2 = parse_float(data.get('sup_edif_m2'))
    usd_m2_terreno = parse_float(data.get('usd_m2_terreno'))
    usd_m2_edif = parse_float(data.get('usd_m2_edif'))
    terreno_total = parse_float(data.get('terreno_total'))
    fot = parse_float(data.get('fot'))
    fos = parse_float(data.get('fos'))
    pisos_maximos = parse_int(data.get('pisos_maximos'))
    valor_dolar = parse_float(data.get('valor_dolar'))
    sup_edif_total = parse_float(data.get('sup_edif_total_calc'))
    porcentaje_emprendimiento = parse_float(data.get('porcentaje_emprendimiento'))
    costo_usd_m2_emprendimiento = parse_float(data.get('costo_usd_m2_emprendimiento'))
    emprendimiento = parse_float(data.get('emprendimiento'))

    total_usd_terreno = terreno_m2 * usd_m2_terreno
    total_usd_edif = sup_edif_m2 * usd_m2_edif
    total_usd = total_usd_terreno + total_usd_edif
    propuesta = parse_float(data.get('propuesta')) or total_usd * 1.10

    gmaps_zona = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    lat, lon = extraer_coordenadas(gmaps_frente or gmaps_zona)

    conn = get_db()
    conn.execute('''
        INSERT INTO valuaciones (
            expediente, caratula, catastro, direccion, fecha,
            terreno_m2, terreno_frente_lado, terreno_antes_revision, usd_m2_terreno,
            sup_edif_m2, edif_frente_lado, edif_antes_revision, usd_m2_edif,
            valor_dolar,
            total_usd_terreno, total_usd_edif, total_usd, propuesta,
            denuncia,
            gmaps_zona, gmaps_frente,
            terreno_total, fot, fos, sup_edif_total, pisos_maximos,
            porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
            observaciones, latitud, longitud,
            creado_por, activa
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('expediente', '').strip(),
        data.get('caratula', '').strip(),
        data.get('catastro', '').strip(),
        data.get('direccion', '').strip(),
        data.get('fecha', ''),
        terreno_m2,
        data.get('terreno_frente_lado', '').strip(),
        data.get('terreno_antes_revision', '').strip(),
        usd_m2_terreno,
        sup_edif_m2,
        data.get('edif_frente_lado', '').strip(),
        data.get('edif_antes_revision', '').strip(),
        usd_m2_edif,
        valor_dolar,
        total_usd_terreno, total_usd_edif, total_usd, propuesta,
        data.get('denuncia', '').strip(),
        gmaps_zona, gmaps_frente,
        terreno_total, fot, fos, sup_edif_total, pisos_maximos,
        porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
        data.get('observaciones', '').strip(),
        lat, lon,
        usuario_actual, 1
    ))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


@app.route('/desactivar/<int:id>', methods=['POST'])
@login_required
def desactivar(id):
    conn = get_db()
    conn.execute(
        'UPDATE valuaciones SET activa = 0, eliminado_por = ?, fecha_eliminacion = ? WHERE id = ?',
        (session['usuario'], datetime.now().strftime('%Y-%m-%d %H:%M'), id)
    )
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


@app.route('/ver/<int:id>')
@login_required
def ver(id):
    conn = get_db()
    valuacion = conn.execute('SELECT * FROM valuaciones WHERE id = ?', (id,)).fetchone()
    valuaciones = conn.execute('SELECT * FROM valuaciones WHERE activa = 1 ORDER BY id DESC').fetchall()
    conn.close()
    if valuacion is None:
        return redirect(url_for('index'))
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template('index.html', valuaciones=valuaciones, today=today,
                           viendo=valuacion, usuario=session['usuario'])


@app.route('/editar/<int:id>')
@login_required
def editar(id):
    conn = get_db()
    valuacion = conn.execute('SELECT * FROM valuaciones WHERE id = ?', (id,)).fetchone()
    valuaciones = conn.execute('SELECT * FROM valuaciones WHERE activa = 1 ORDER BY id DESC').fetchall()
    conn.close()
    if valuacion is None:
        return redirect(url_for('index'))
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template('index.html', valuaciones=valuaciones, today=today,
                           editando=valuacion, usuario=session['usuario'])


@app.route('/actualizar/<int:id>', methods=['POST'])
@login_required
def actualizar(id):
    data = request.form
    usuario_actual = session['usuario']

    terreno_m2 = parse_float(data.get('terreno_m2'))
    sup_edif_m2 = parse_float(data.get('sup_edif_m2'))
    usd_m2_terreno = parse_float(data.get('usd_m2_terreno'))
    usd_m2_edif = parse_float(data.get('usd_m2_edif'))
    terreno_total = parse_float(data.get('terreno_total'))
    fot = parse_float(data.get('fot'))
    fos = parse_float(data.get('fos'))
    pisos_maximos = parse_int(data.get('pisos_maximos'))
    valor_dolar = parse_float(data.get('valor_dolar'))
    sup_edif_total = parse_float(data.get('sup_edif_total_calc'))
    porcentaje_emprendimiento = parse_float(data.get('porcentaje_emprendimiento'))
    costo_usd_m2_emprendimiento = parse_float(data.get('costo_usd_m2_emprendimiento'))
    emprendimiento = parse_float(data.get('emprendimiento'))

    total_usd_terreno = terreno_m2 * usd_m2_terreno
    total_usd_edif = sup_edif_m2 * usd_m2_edif
    total_usd = total_usd_terreno + total_usd_edif
    propuesta = parse_float(data.get('propuesta')) or total_usd * 1.10

    gmaps_zona = data.get('gmaps_zona', '').strip()
    gmaps_frente = data.get('gmaps_frente', '').strip()
    lat, lon = extraer_coordenadas(gmaps_frente or gmaps_zona)

    conn = get_db()
    conn.execute('''
        UPDATE valuaciones SET
            expediente=?, caratula=?, catastro=?, direccion=?, fecha=?,
            terreno_m2=?, terreno_frente_lado=?, terreno_antes_revision=?, usd_m2_terreno=?,
            sup_edif_m2=?, edif_frente_lado=?, edif_antes_revision=?, usd_m2_edif=?,
            valor_dolar=?,
            total_usd_terreno=?, total_usd_edif=?, total_usd=?, propuesta=?,
            denuncia=?,
            gmaps_zona=?, gmaps_frente=?,
            terreno_total=?, fot=?, fos=?, sup_edif_total=?, pisos_maximos=?,
            porcentaje_emprendimiento=?, costo_usd_m2_emprendimiento=?, emprendimiento=?,
            observaciones=?, latitud=?, longitud=?,
            editado_por=?
        WHERE id=?
    ''', (
        data.get('expediente', '').strip(),
        data.get('caratula', '').strip(),
        data.get('catastro', '').strip(),
        data.get('direccion', '').strip(),
        data.get('fecha', ''),
        terreno_m2,
        data.get('terreno_frente_lado', '').strip(),
        data.get('terreno_antes_revision', '').strip(),
        usd_m2_terreno,
        sup_edif_m2,
        data.get('edif_frente_lado', '').strip(),
        data.get('edif_antes_revision', '').strip(),
        usd_m2_edif,
        valor_dolar,
        total_usd_terreno, total_usd_edif, total_usd, propuesta,
        data.get('denuncia', '').strip(),
        gmaps_zona, gmaps_frente,
        terreno_total, fot, fos, sup_edif_total, pisos_maximos,
        porcentaje_emprendimiento, costo_usd_m2_emprendimiento, emprendimiento,
        data.get('observaciones', '').strip(),
        lat, lon,
        usuario_actual,
        id
    ))
    conn.commit()
    conn.close()
    return redirect(url_for('index'))


if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5002)
