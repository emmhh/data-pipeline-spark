import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import boto3
from urllib.parse import urlparse
import json
from datetime import datetime

"""
v5 - handles the cases where input folder is empty
v6 - hanldes .OK file after processing all the files( remove the .OK file)
"""
def delete_ok_file(s3_client, ok_file_path):
    """Delete the .OK file from S3 after processing."""
    parsed_ok_file = urlparse(ok_file_path)
    s3_client.delete_object(Bucket=parsed_ok_file.netloc, Key=parsed_ok_file.path.lstrip('/'))
    print(f".OK file {ok_file_path} deleted after processing.")

def move_s3_file(s3_client, source, destination):
    """Move file from source to destination in S3."""
    parsed_source = urlparse(source)
    parsed_dest = urlparse(destination)

    # Copy the object to the new location
    copy_source = {'Bucket': parsed_source.netloc, 'Key': parsed_source.path.lstrip('/')}
    s3_client.copy(copy_source, parsed_dest.netloc, parsed_dest.path.lstrip('/'))

    # Delete the original object
    s3_client.delete_object(Bucket=parsed_source.netloc, Key=parsed_source.path.lstrip('/'))

def write_schema_to_s3(schema, s3_folder_path):
    """Write DataFrame schema to a JSON file in S3 with the current date and time in the file name."""
    # Create a schema JSON structure
    schema_json = [
        {"Name": field.name, "Type": str(field.dataType)}
        for field in schema
    ]
    
    # Generate a filename with the current date and time
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file_name = f"schema_{current_time}.json"
    
    # Parse the S3 folder path
    parsed_s3_path = urlparse(s3_folder_path)
    bucket_name = parsed_s3_path.netloc
    folder_prefix = parsed_s3_path.path.lstrip('/')
    
    # Define the full path for the JSON file in S3
    json_file_key = f"{folder_prefix}/{json_file_name}"
    
    # Initialize the S3 client
    s3_client = boto3.client('s3')
    
    # Upload the JSON schema to the specified S3 location
    s3_client.put_object(
        Bucket=bucket_name,
        Key=json_file_key,
        Body=json.dumps(schema_json, indent=2)
    )
    
    print(f"Schema written to s3://{bucket_name}/{json_file_key}")

def main(input_directory, output_directory, processed_directory, schema_json_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV to Parquet") \
        .getOrCreate()

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # Parse the input directory to get bucket and prefix
    parsed_input = urlparse(input_directory)
    bucket_name = parsed_input.netloc
    prefix = parsed_input.path.lstrip('/')

    # List objects in the input directory
    print(f"Listing objects in s3://{bucket_name}/{prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    print("!!!!", response)
    
    # # Delete the OK file since we are processing it.
    # ok_file_path = f"{input_directory}/.OK"
    # delete_ok_file(s3_client, ok_file_path)

    # Filter out any directories and check for actual CSV files
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Size'] > 0 and obj['Key'].endswith('.csv')]
    if not files:
        print("No CSV files found in the input directory. Exiting.")
        sys.exit(0)

    # Define the schema for the initial metadata rows
    meta_schema = StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ])

    # Use Spark to read all CSV files from the S3 bucket
    csv_files_df = spark.read.format("csv").option("header", "false").schema(meta_schema).load(input_directory + "/*.csv")
    # csv_files_df = spark.read.format("csv").option("header", "false").schema(meta_schema).load([f"s3://{bucket_name}/{file_key}" for file_key in files])
    # Get distinct file paths from the DataFrame to process each file separately
    file_paths = csv_files_df.selectExpr("input_file_name() as path").distinct().collect()

    for file_path in file_paths:
        current_file = file_path['path']
        print(f"Processing file: {current_file}")

        # Read metadata from the CSV file
        meta_df = spark.read.csv(current_file, schema=meta_schema, header=False).limit(7)

        # Extract metadata
        metadata = {row.key: row.value for row in meta_df.collect()}

        # Use .get() with a fallback to an empty string if the key is not present
        file_name = metadata.get('File', '')
        patient_name = metadata.get('PatientName', '')
        patient_id = metadata.get('PatientID', '')
        patient_birth_date = metadata.get('PatientBirthDate', '')
        test_date = metadata.get('TestDate', '')
        test_time = metadata.get('TestTime', '')

        # Ensure the value is not None before calling .strip()
        file_name = file_name.strip() if file_name else ''
        patient_name = patient_name.strip() if patient_name else ''
        patient_id = patient_id.strip() if patient_id else ''
        patient_birth_date = patient_birth_date.strip() if patient_birth_date else ''
        test_date = test_date.strip() if test_date else ''
        test_time = test_time.strip() if test_time else ''

        # Read the CSV file and filter out the metadata rows
        full_df = spark.read.csv(current_file, header=False, inferSchema=True)
        # Filter out the first 7 rows (metadata) to get the data
        data_df = full_df.rdd.zipWithIndex().filter(lambda x: x[1] > 7).map(lambda x: x[0]).toDF(full_df.schema)

        # Extract column headers from row 8
        actual_headers = full_df.rdd.zipWithIndex().filter(lambda x: x[1] == 7).map(lambda x: x[0]).first()

        # Rename columns using the actual headers
        data_df = data_df.toDF(*actual_headers)

        # Add metadata as columns to the data DataFrame
        data_df = data_df.withColumn("PatientName", lit(patient_name)) \
                         .withColumn("PatientID", lit(patient_id)) \
                         .withColumn("PatientBirthDate", lit(patient_birth_date)) \
                         .withColumn("TestDate", lit(test_date)) \
                         .withColumn("TestTime", lit(test_time))

        # Multiply by 1e5 to convert back to the original Unix timestamp and cast as a Timestamp
        data_df = data_df.withColumn(
            "Timestamp",
            (col("ClockDateTime") * 1e5).cast(TimestampType())
        )

        # Partition by Year, Month, and Day
        data_df = data_df.withColumn("Year", year(col("Timestamp"))) \
                         .withColumn("Month", month(col("Timestamp"))) \
                         .withColumn("Day", dayofmonth(col("Timestamp")))

        write_schema_to_s3(data_df.schema, schema_json_path)

        # Load existing Parquet data if it exists and append new data
        try:
            existing_df = spark.read.parquet(output_directory)
            combined_df = existing_df.union(data_df)
        except Exception as e:
            print(f"No existing parquet files found for {file_name}, creating new data. ({e})")
            combined_df = data_df

        # Write the combined DataFrame to Parquet with partitioning by Year, Month, and Day
        combined_df.write \
            .partitionBy("Year", "Month", "Day") \
            .parquet(output_directory, mode="append")

        # Move processed file to a new location
        parsed_current_file = urlparse(current_file)
        file_key = parsed_current_file.path.lstrip('/')
        processed_file_path = f"{processed_directory}/{file_key.split('/')[-1]}"
        move_s3_file(s3_client, current_file, processed_file_path)

        

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    # Initialize argument parser
    parser = argparse.ArgumentParser(description="Convert CSV files to Parquet with partitioning by Year, Month, and Day.")
    parser.add_argument("--input_directory", type=str, required=True, help="The directory containing input CSV files.")
    parser.add_argument("--output_directory", type=str, required=True, help="The directory to store output Parquet files.")
    parser.add_argument("--processed_directory", type=str, required=True, help="The directory to move processed CSV files.")
    parser.add_argument("--schema_json_path", type=str, required=True, help="The path to store the JSON schema file.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Run the main function with parsed arguments
    main(args.input_directory, args.output_directory, args.processed_directory, args.schema_json_path)
