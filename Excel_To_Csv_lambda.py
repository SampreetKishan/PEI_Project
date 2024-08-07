import json
import io
from urllib.parse import unquote_plus
import boto3
import pandas as pd
import random

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    if event:
        s3_records = event["Records"][0]
        bucket_name = str(s3_records["s3"]["bucket"]["name"])
        file_name = unquote_plus(str(s3_records["s3"]["object"]["key"]))
        print("Bucket name:", bucket_name)
        print("File name:", file_name)
        file_obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        file_content = file_obj["Body"].read()
        read_excel_data = io.BytesIO(file_content)
        df = pd.read_excel(read_excel_data)
        print("The dataframe is: ")
        print(df.head(10))
        
        df.to_csv("/tmp/updated.csv", index=False)
        
        new_file_name = "customers/updated" + str(random.randint(1, 1000)) + "_" + str(random.randint(2000, 4000)) +".csv"
        print("New file name: ", new_file_name)
        s3_resource.Bucket("peiproject-sampy").upload_file("/tmp/updated.csv", new_file_name)
        

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}
