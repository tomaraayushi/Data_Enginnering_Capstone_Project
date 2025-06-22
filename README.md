# Data_Engineering_Capstone_Project
Final assignment for the course "Data Modeling, Transformation, and Serving" by DeepLearning.AI & AWS on Coursera.

## Project Overview
![Technical_Overview](https://github.com/tomaraayushi/Data_Enginnering_Capstone_Project/blob/main/assets/Capstone-diagram2.png)

This project implements an end-to-end data engineering pipeline on AWS Cloud, orchestrated using Apache Airflow. Data is extracted from an API endpoint and an RDS database, then stored in S3 in the Landing Zone. The raw data is transformed using Apache Iceberg within the Transformation Zone and cataloged with AWS Glue. Redshift Spectrum is used for querying data, and quality checks are applied to ensure accuracy. The transformed data is loaded into Amazon Redshift for analytics. Finally, dbt is used for modeling, and Apache Superset provides interactive dashboards for visualization.

## Author(s)

* Joe Reis
* Morgan Willis
* Navnit Shukla



