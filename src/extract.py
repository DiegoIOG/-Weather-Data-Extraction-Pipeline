import requests
import os
from dotenv import load_dotenv

load_dotenv()



def get_cities() -> list:
    """
    Lee la variable CITIES del .env y la convierte en lista de dicts.
    Formato esperado: Nombre:lat:lon,Nombre:lat:lon
    """
    raw = os.getenv("CITIES", "")

    if not raw:
        raise ValueError(" La variable CITIES no está definida en el .env")

    cities = []
    for entry in raw.split(","):
        parts = entry.strip().split(":")
        if len(parts) != 3:
            print(f"    Entrada inválida ignorada: {entry}")
            continue
        cities.append({
            "name": parts[0].strip(),
            "lat":  float(parts[1]),
            "lon":  float(parts[2]),
        })

    return cities



def get_weather(city: dict) -> dict:
    """
    Consulta Open-Meteo para una ciudad y regresa los datos limpios.
    No requiere API key.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={city['lat']}"
        f"&longitude={city['lon']}"
        f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
        f"&timezone=America/Monterrey"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        print(f"  Timeout al consultar {city['name']}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"   Error al consultar {city['name']}: {e}")
        return None

    data    = response.json()
    current = data["current"]

    return {
        "city":        city["name"],
        "latitude":    city["lat"],
        "longitude":   city["lon"],
        "recorded_at": current["time"],
        "temperature": current["temperature_2m"],
        "humidity":    current["relative_humidity_2m"],
        "wind_speed":  current["wind_speed_10m"],
    }



def extract_all() -> list:
    """
    Itera todas las ciudades del .env y regresa
    una lista de registros listos para cargar.
    """
    cities  = get_cities()
    records = []

    for city in cities:
        print(f"   Consultando {city['name']}...")
        record = get_weather(city)

        if record:
            records.append(record)
            print(f"     {record['temperature']}°C | "
                  f"{record['humidity']}% humedad | "
                  f"{record['wind_speed']} km/h viento")
        else:
            print(f"      Se omitió {city['name']} por error")

    return records