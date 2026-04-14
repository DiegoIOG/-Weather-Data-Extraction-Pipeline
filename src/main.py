
from extract import extract_all
from load import load_all


def main():
    print("=" * 50)
    print("    Weather → Snowflake Pipeline")
    print("=" * 50)

    
    print("\n Extrayendo datos de Open-Meteo...")
    records = extract_all()

    if not records:
        print("\n No se obtuvieron datos. Revisa tu .env o conexión.")
        return

    print(f"\n {len(records)} ciudades extraídas correctamente.")

    
    print("\n Cargando datos en Snowflake...")
    load_all(records)

    print("\n" + "=" * 50)
    print("   Pipeline finalizado exitosamente!")
    print("=" * 50)


if __name__ == "__main__":
    main()