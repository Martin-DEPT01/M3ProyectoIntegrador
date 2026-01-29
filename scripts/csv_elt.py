import pandas as pd
from io import StringIO
import mysql_connection
import s3_connection

def main():

    bucket_name="m3-pi-raw-data"
    csv_file="AB_NYC.csv"

    # Descargamos el archivo CVS como string desde el bucket
    s3 = s3_connection.connect_to_s3()
    response = s3.get_object(Bucket=bucket_name, Key=csv_file)
    csv_data = response["Body"].read().decode("utf-8")

    # Cargamos el CSV a la tabla. Si la misma no existe la crea
    df_csv_data = pd.read_csv(StringIO(csv_data))
    mysql_table_name = "mysql_csv_raw"
    mysql_conn_engine = mysql_connection.get_mysql_engine()

    df_csv_data.to_sql(mysql_table_name, con=mysql_conn_engine, if_exists="replace", index=False)
    print("✅ Archivo csv cargado exitosamente")

# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    main()