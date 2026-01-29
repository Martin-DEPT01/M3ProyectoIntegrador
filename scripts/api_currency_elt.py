import pandas as pd
import json
import mysql_connection
import s3_connection
from datetime import date, timedelta

def main():

    hoy = date.today()
    ayer = hoy - timedelta(days=1)

    bucket_name="m3-pi-raw-data"
    json_key=f"cotizacion_dolar/{ayer}.json"

    # Descargamos el json desde S3
    s3 = s3_connection.connect_to_s3()
    json_obj = s3.get_object(Bucket=bucket_name, Key=json_key)
    json_body = json_obj["Body"].read().decode("utf-8")
    json_data = json.loads(json_body)

    # Convertimos el json a DataFrame
    df_json_data = pd.DataFrame(json_data)
    
    # Renombramos los campos
    df_json_data.rename(
        columns={
            "idVariable": "id_moneda",
            "valor": "cotizacion_en_pesos"
        },
        inplace=True,
    )

    sql_table_name = "mysql_currency_raw"
    mysql_conn_engine = mysql_connection.get_mysql_engine()

    # Hacemos un append de los datos
    df_json_data.to_sql(sql_table_name, con=mysql_conn_engine, if_exists="append", index=False)

    print(f"✅ Datos de cotizacion del dia {ayer} appendados")


# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    main()