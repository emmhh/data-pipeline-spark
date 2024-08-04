import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DateType
import boto3
from urllib.parse import urlparse
from datetime import datetime
import json

"""
v5 - handles the cases where input folder is empty
v6 - handles .OK file after processing all the files (remove the .OK file)
v7 - type casting, but REALLY slow, decide to move to predefined schema in v8
v8 - predefined schema, no type casting
v9 - dynamically reads header row for schema definition
"""

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
    
    # Filter out any directories and check for actual CSV files
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Size'] > 0 and obj['Key'].endswith('.csv')]
    if not files:
        print("No CSV files found in the input directory. Exiting.")
        sys.exit(0)

    for file_key in files:
        file_path = f"s3://{bucket_name}/{file_key}"
        print(f"Processing file: {file_path}")

        # Read the CSV file to extract header row (row 8)
        header_df = spark.read.csv(file_path, header=False, inferSchema=False).limit(8)
        header_row = header_df.collect()[7]  # Get the 8th row as the header

        # # Define the schema based on the actual header row
        # schema = StructType(
        #     [StructField(header_row[i], TimestampType() if i == 0 else DoubleType(), True) for i in range(len(header_row))]
        # )
        # Define the schema based on the actual header row
        schema = StructType(
            [StructField(header_row[i], DoubleType(), True) for i in range(len(header_row))]
        )

        # Read the CSV file with the defined schema
        full_df = spark.read.csv(file_path, header=False, schema=schema)

        # Add metadata columns explicitly
        data_df = full_df.withColumn("PatientName", lit("").cast(StringType())) \
                         .withColumn("PatientID", lit("").cast(StringType())) \
                         .withColumn("PatientBirthDate", lit("").cast(DateType())) \
                         .withColumn("TestDate", lit("").cast(DateType())) \
                         .withColumn("TestTime", lit("").cast(StringType()))

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
            print(f"No existing parquet files found for {file_path}, creating new data. ({e})")
            combined_df = data_df

        # Write the combined DataFrame to Parquet with partitioning by Year, Month, and Day
        combined_df.write \
            .partitionBy("Year", "Month", "Day") \
            .parquet(output_directory, mode="append")

        # Move processed file to a new location
        move_s3_file(s3_client, file_path, f"{processed_directory}/{file_key.split('/')[-1]}")

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
