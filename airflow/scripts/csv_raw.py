import os
import s3_connection as s3_conn
#import pandas as pd

def main():

    # cargo csv desde /data
    #csv_data = pd.read_csv("../data/AB_NYC.csv")

    CSV_PATH = os.getenv("DATA_PATH")

    bucket_name = "m3-pi-bucket"
    file_path = f"{CSV_PATH}/AB_NYC.csv"
    s3_path = "NYC_CSV/AB_NYC.csv"


    s3 = s3_conn.connect_to_s3()

    s3.upload_file(file_path, bucket_name, s3_path)

    print(f"✅Archivo subido a s3://{bucket_name}/{s3_path}")


# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    main()



