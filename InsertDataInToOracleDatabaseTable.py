import csv
import cx_Oracle
import boto3
from moto import mock_aws
import requests
from io import StringIO

# Function to upload data from URL to S3 bucket
def upload_data_to_s3_from_url(url, bucket_name, object_key):
    response = requests.get(url)
    data = response.content

    s3 = boto3.client('s3')
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key=object_key, Body=data)

    print(f"Data from URL '{url}' uploaded to S3 bucket '{bucket_name}' with key '{object_key}'.")

# Function to read data from S3 bucket
def read_data_from_s3(bucket_name, object_key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data_content = response['Body'].read()
    return data_content

# Function to insert data into Oracle database
def insert_data_into_oracle(data, table_name):
    # Database connection details
    user = 'hr'
    password = 'hr'
    dsn = cx_Oracle.makedsn('localhost', 1521, service_name='orclpdb')

    try:
        # Connect to the database
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()

        # Create a CSV reader from the data content
        csv_data = data.decode('utf-8')
        csv_reader = csv.reader(StringIO(csv_data))

        # Skip the header
        next(csv_reader)

        # Construct the INSERT statement
        insert_statement = f"INSERT INTO {table_name} VALUES ({', '.join([':' + str(i + 1) for i in range(len(next(csv_reader)))])})"

        # Reset the CSV reader
        csv_data = data.decode('utf-8')
        csv_reader = csv.reader(StringIO(csv_data))

        # Skip the header row
        next(csv_reader)

        # Execute the INSERT statement for each row
        for row_number, row in enumerate(csv_reader, start=2):  # Start from row 2 (1-based index)
            try:
                cursor.execute(insert_statement, row)
            except cx_Oracle.Error as insert_error:
                # Print error details including row number and value causing the error
                print(f"Error inserting data into Oracle database at row {row_number}: {insert_error}")
                print(f"Problematic row data: {row}")

        # Commit the transaction
        connection.commit()

        print("Data inserted into Oracle database.")

    except cx_Oracle.Error as error:
        print("Error connecting to Oracle database:", error)

    finally:
        # Close cursor and connection
        cursor.close()
        connection.close()


# URL of the CSV file
url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2021-financial-year-provisional/Download-data/annual-enterprise-survey-2021-financial-year-provisional-csv.csv'

# Name of the S3 bucket
bucket_name = 'my_bucket'

# Key (object key) under which the file will be stored in the S3 bucket
object_key = 'data.csv'

# Name of the table in Oracle database
table_name = 'data13'

# Start the moto mock S3 server
with mock_aws():
    # Upload data to S3 from URL
    upload_data_to_s3_from_url(url, bucket_name, object_key)
    # Read the uploaded data from S3
    s3_data = read_data_from_s3(bucket_name, object_key)
    # Insert data into Oracle database
    insert_data_into_oracle(s3_data, table_name)

