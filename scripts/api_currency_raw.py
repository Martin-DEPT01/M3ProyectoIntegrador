import requests
from datetime import date, timedelta
import json
import s3_connection


def main():

    hoy = date.today()
    ayer = hoy - timedelta(days=1)

    #fecha_formateada = hoy.strftime("%Y-%m-%d")
    fecha_formateada = ayer.strftime("%Y-%m-%d")
    
    
    star_date = fecha_formateada
    end_date = fecha_formateada

    # id de divisa
    id_currency = 4


    url = f"https://api.bcra.gob.ar/estadisticas/v3.0/monetarias/{id_currency}?desde={star_date}&hasta={end_date}"


    try:
        response = requests.get(url, verify=False)

        if response.status_code == 200:
            print("✅ Conexión exitosa")
            data = response.json()["results"]
            print(data)  # o data["data"]
        else:
            print(f"⚠️ Error en la respuesta. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión: {e}")

    
    # Subir el json al bucket
    s3 = s3_connection.connect_to_s3()

    bucket_name="m3-pi-raw-data"
    s3_key=f"cotizacion_dolar/{ayer}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(data),
    )

    print(f"✅ Archivo subido a s3://{bucket_name}/{s3_key}")
    
# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    main()