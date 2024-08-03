https://docs.google.com/document/d/129XCwxpDuanK8i2NmHq00VVBzfDTSufX_fPHh8Laxis/edit#heading=h.82tnjwl94wdi

everything below is deprecated, please refer to the google doc above.
# Medical Data Sharing Platform
 - ~500TB data size
 - Queries - data filering & aggregation
 - structured data
 - New data come in monthly
# Solution Analysis
- Data Source
    - monthly from hospitals in .csv format
    - highly structured, each .csv contains massive categories of medical test results.
    Since its monthly updated, building a pipeline might not be worthy.
- Data Processing
    - option 1: SQL DB storage + Data Processer ( Apache Spark / Hadoop), Spark is getting more popular and supports both batch and stream processing with wide variety of programming languages, so i personally prefer Spark.
    - option 2: Might use Data Lake (plain CSV) + Apache Spark; SQL is not necessary espcecially in our case we dont have CRUD requirements / transaciton consistency requirements. SQL only adds unncessary complexity at this point. 
        - Pros:
            - scalability:
                - Data lakes (e.g., Amazon S3) are designed to handle massive amounts of data and can scale horizontally with ease.
                - Apache Spark is optimized for distributed data processing and can efficiently handle large datasets.
            - Performance:
                - Using columnar storage formats (e.g., Parquet, ORC) in the data lake improves read performance.
                - Spark can process data in parallel, leveraging the distributed nature of the data lake.
            - Simplicity:
                - Directly storing data in a data lake and processing it with Spark simplifies the data pipeline.
                - Reduces the need for intermediate data storage and retrieval steps.
        - Cons:
            - Data Management:
                - Managing data integrity, consistency, and schema evolution can be more challenging in a data lake compared to a SQL database.
            - Query Optimization:
                - While Spark can perform efficient queries, it may not have the same level of query optimization and indexing capabilities as a SQL database.
- Data Consumption ( if needed )
    - download with no visualization (?kinda sucks)
    - either use some existing tools to visualise data, 
    - or React + material UI (instead of showing the whole table, show the key points that the user cares about to avoid load massive data on browswer. And pagenization.)
    - might have more
- Censor identifiable patient personal information, store the mapper on a local machine (maps fake [patient ID, Name] from real [patient ID, Name])
- Administraion System? If needed.

# Final Architecture
- Web App simply manages
    - the User Authorization, 
    - sending specific query request to Spark
    - displaying of queried result
    - allowing download.
    - Simple SQL for user management? Django/Flask for lightweight backend? NextJS + Material UI for simple frontend?
        - without having to purchase SSL, we could try AWS Amplify for hosting, which provides free SSL, and with backend, we could use AWS Lambda + API Gateway.
- raw/original csv data is stored in DataLake - AWS S3
- Apache Spark 
    - query the process 
    - store in S3
    - Generate temporary pre-signed URLs for secure download from S3. These URLs can be set to expire after a certain period, enhancing security.