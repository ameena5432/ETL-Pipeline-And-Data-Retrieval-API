ETL Pipeline & Data Retrieval API


This project comprises an Extract, Transform, Load (ETL) pipeline designed to process JSON data and populate a PostgreSQL database. Additionally, it includes an API endpoint to retrieve data from the database based on a specific key.


Overview

This project involves the development of an ETL pipeline in Python, utilizing SQLAlchemy and PostgreSQL, to process JSON data files. 
Additionally, it implements a FastAPI-based API endpoint to fetch specific data from the populated database.

ETL Pipeline

Schema Definition

The data schema is defined using SQLAlchemy's declarative Base. The schema includes fields such as id, key, title, last_modified_i, has_fulltext, and first_publish_year.

Transformation Process

The extract_transform_load function is responsible for transforming JSON data into the required format before inserting it into the database.

Handling Large Volumes

Efforts have been made to optimize file handling and database insertion processes for handling large volumes of data efficiently.

Data Storage System

PostgreSQL has been chosen as the data store for this project. Proper indexing on frequently queried fields and efficient data insertion methods have been implemented for optimal data retrieval and storage.

Modularity and Extensibility

The code is structured in a modular fashion, allowing for easy addition of new ETL features and enhancements.


API Endpoint

Endpoint Design

The API includes an endpoint /get_data that accepts a key as input and retrieves relevant data from the BookData table within the last 90 days.

Response Time

Efforts have been made to optimize database queries, indexing, and retrieval methods to ensure faster response times for the API.

Recommendations

Optimization: Further optimization of database queries and indexing for improved performance.
Scalability: Consider scalability options to handle the daily processing of large volumes of data.
Further Steps
Refactor code for clearer separation of concerns and better maintainability.
Optimize database queries and indexing for enhanced performance.
Implement scalability strategies for handling larger data volumes efficiently.
