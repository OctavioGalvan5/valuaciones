import re
import math

def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia en km entre dos puntos GPS usando la fórmula de Haversine."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))

def extraer_coordenadas(url):
    """Extrae latitud y longitud de un link de Google Maps."""
    if not url:
        return None, None

    # PRIORIDAD 1: Coordenadas del lugar real en parámetros !3d y !4d
    # Usamos findall para obtener TODAS las ocurrencias y tomar la última (la más específica)
    lat_matches = re.findall(r'!3d(-?[\d.]+)', url)
    lon_matches = re.findall(r'!4d(-?[\d.]+)', url)
    if lat_matches and lon_matches:
        # Tomamos las últimas coordenadas encontradas, que son las del lugar específico
        return float(lat_matches[-1]), float(lon_matches[-1])

    return None, None

# URLs de prueba
url_tomasi = "https://www.google.com/maps/place/Supermercado+tomasi/@-7.7128994,-47.3320745,14.57z/data=!3m1!5s0x92d7f074112b35f3:0x77f6f1900c923038!4m14!1m7!3m6!1s0x941bc24cd2bdea6d:0x5a63a2a22624dad1!2sPlaza+Alvarado!8m2!3d-24.7892235!4d-65.4283499!16s%2Fg%2F11c52j_x2c!3m5!1s0x92d7f1d818eb1a57:0x28693ac4be843b6b!8m2!3d-7.7101973!4d-47.3161703!16s%2Fg%2F11fnn8vs6d?entry=ttu&g_ep=EgoyMDI2MDMwMi4wIKXMDSoASAFQAw%3D%3D"

url_alvarado = "https://www.google.com/maps/place/Plaza+Alvarado/@-24.7919454,-65.4324549,16z/data=!4m6!3m5!1s0x941bc24cd2bdea6d:0x5a63a2a22624dad1!8m2!3d-24.7892235!4d-65.4283499!16s%2Fg%2F11c52j_x2c?entry=ttu&g_ep=EgoyMDI2MDIyNS4wIKXMDSoASAFQAw%3D%3D"

# Extraer coordenadas
lat1, lon1 = extraer_coordenadas(url_tomasi)
lat2, lon2 = extraer_coordenadas(url_alvarado)

print(f"Tomasi: lat={lat1}, lon={lon1}")
print(f"Alvarado: lat={lat2}, lon={lon2}")

if lat1 and lon1 and lat2 and lon2:
    dist_km = haversine(lat1, lon1, lat2, lon2)
    print(f"\nDistancia: {dist_km:.2f} km ({dist_km*1000:.0f} metros)")
