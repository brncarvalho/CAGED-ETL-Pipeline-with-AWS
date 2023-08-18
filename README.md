
# Data Pipeline Project: ETL and AWS Analytics for CAGED Data

My primary goal with this the project was to get acquainted with AWS resources to extract, process, and analyze the employment-related data from CAGED (Cadastro Geral de Empregados e Desempregados). CAGED stands for General Registry of Employed and Unemployed Persons, which is a system used by the brazilian Ministry of Labor and Employment to record hirings and layoffs of workers in Brazil. It was created with the purpose of providing detailed information for the development of public policies and statistics about the job market. For the analytics purpose of this project, it was extracted data from 2010 to 2023, total of 156 files and 50.2 GB size.


It’s important to mention that since January 2020, the use of the General Registry of Employed and Unemployed Persons (Caged) system has been replaced by the Digital System of Fiscal, Social Security, and Labor Obligations Reporting (eSocial) for some companies, as established by Decree SEPRT No. 1,127 of October 14, 2019. This system replacement caused some changes in the data structure and mapping nomenclature when compared with data previous from 2020. Therefore, the analytics purpose of grabbing data from 2010 to 2019 was to build a time series model and do some forecasting regarding the monthly balance amount (monthly difference of the amount of hirings and layoffs) for 2024 and 2025.

# Data Source

It's important to note that connections based on the File Transfer Protocol (FTP) have ceased to be supported by several browsers. Therefore, if you want to perform the download directly through the link (ftp://ftp.mtps.gov.br/pdet/microdados/), it's necessary to verify compatibility with this protocol. 

For browser-based access, the Ministry of Labor recommends using Microsoft Edge configured in Internet Explorer mode or use FileZilla Server application for Windows and ProFTPd for Unix users adjusting the connection to the server ftp://ftp.mtps.gov.br/pdet/microdados/


## Project Structure Overview

![App Screenshot](https://live.staticflickr.com/65535/53118120996_0290a2571d_z.jpg)


### Data Extraction and Ingestion into S3: 
The raw_ingestion.pynb is employed to access the FTP server and download the CAGED data files, which are in a compressed format (.7z). The ftplib and urllib.request libraries facilitate this data retrieval process. The downloaded data files are ingested into the Amazon S3 data lake at Raw Zone using the boto3 library. The data then is organized into two distinct folders: "compressed" and "uncompressed," each serving a specific purpose in the data processing pipeline.

![App Screenshot](https://live.staticflickr.com/65535/53122927769_620db69f01_z.jpg)

The uncompressing.pynb is designed to extract data from the "compressed" folder on S3, decompress it, and store the resulting CSV files in the "uncompressed" folder. This process transforms the data from its initial compressed format to more accessible CSV format.

![App Screenshot](https://live.staticflickr.com/65535/53123213033_1641c13c8d_z.jpg)

### Bronze Zone Ingestion with AWS Glue: 

The purpose of the bronze zone is to prepare the files to be further processed and transformed with Apache Spark. Since the files are provided in CSV format and Spark engine works better with Parquet due its columnar storage format, i created a AWS Glue Job to read the CSV files from the raw zone, convert them into Parquet format and then ingest into bronze zone, partitioned by month and year for future optimized data querying. 


The AWS Glue is a fully managed ETL Service with Spark Engine that consists of Central Metadadata Repository as illustrated in the diagram below, serving as a fully managed serverless ETL Tool. 
This removed the overhead and barriers to entry since i was able to perform an ETL service in AWS without worrying about infrastructure. 
 

![App Screenshot](https://docs.aws.amazon.com/images/glue/latest/dg/images/HowItWorks-overview.png)


### ETL and Data Modeling with AWS Glue & Apache Spark: 
Leveraging AWS Glue's capabilities, the etl_new_caged.ipynb involves executing transformations on the data using Apache Spark within AWS Glue Interactive Session. These transformations lead to the creation of dimensional and fact tables that serve as foundational components for data analysis.

![App Screenshot](https://live.staticflickr.com/65535/53119025370_f19f2d96fe_z.jpg) 


### Silver Zone ingestion: 

The results of the ETL process – the dimensional and fact tables – are stored in the zone of the Data Lake on Amazon S3, each table with its particular partitions to optimize data querying. This zone houses processed and structured data (tables) ready for consuming.

### Cataloging and Querying:

The AWS Glue Crawler is employed to catalog the processed data from silver zone into AWS Glue Catalog. The Crawler is a program that connects to a data store (source or target), progresses through a prioritized list of classifiers to determine the schema of the data, and then creates metadata tables in the AWS Glue Data Catalog. This enabled seamless integration with AWS Athena and Redshift (Data Warehouse) for initial data queries.


### Data Visualization: 
The processed data is visualized using the Power BI tool. A connection is established between Power BI and AWS Athena using the ODBC driver. This connection facilitated the creation of insightful visualizations and reports based on the queried data.

![App Screenshot](https://live.staticflickr.com/65535/53121466130_a7a616614b_c.jpg)

### Further Exploration and Analysis: 
Beyond Power BI, the project involves additional exploration of data using PyAthena. PyAthena is a Python library that allows to connect to and query Amazon Athena using Python. With PyAthena, i was able to run SQL queries on my data and use Pandas to analyze the results of the queries. Below i tested a forecasting using the ARIMA (Autoregressive Integrated Moving Averages) model. The notebook is available here.  

![App Screenshot](https://live.staticflickr.com/65535/53121302035_ca0c1832b0_c.jpg)
