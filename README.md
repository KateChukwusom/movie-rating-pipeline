# Movie Ratings Data Engineering Pipeline Project

This project involves building an end-to-end pipeline for processing and analyzing movie ratings from a movie lens data set.

# Project Overview
The goal is to create an automated system that;
- Takes raw movie rating data from csv files
- Processes and transforms the data
- Stores it efficiently
- Performs analyis and Visualizations
- Handles new data incrementally based on movie release dates

# Dataset Components
The project works with three main csv files;
- Data_movie_lens.csv: which contains individual ratings for movies, each row represents one ratings given by one user to one movie.

- item_movie_lens.csv: which contains meta data about the movies. This is a dimension table.

- User_movie_lens.csv: This contains data about the user who provides the ratings. Another dimension table.

  # Pipeline Components  

| Component       | Technology     | Version  |
|-----------------|---------------|----------|
| Orchestration   | Apache Airflow | 2.6.3    |
| Data Warehouse  | Snowflake      | -        |
| Storage         | AWS S3         | -        |
| Visualization   | Power BI       | -        |
| OS              | Ubuntu         | 22.04 LTS|

