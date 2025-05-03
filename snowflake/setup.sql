create warehouse MDP_WH;
create database MD_DB;
create schema MD_SCH;

CREATE OR REPLACE STORAGE INTEGRATION MOVIE_DATA_STAGE
  TYPE =EXTERNAL_STAGE
   STORAGE_PROVIDER = S3
   ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::905418429000:role/s3tosnowflakeroledeckate'
   STORAGE_ALLOWED_LOCATIONS = ('s3://dec-kate--lightat2003/transformed/');

DESC STORAGE INTEGRATION MOVIE_DATA_STAGE;

CREATE STAGE MDP_STAGE
URL = 's3://dec-kate--lightat2003/transformed/'
STORAGE_INTEGRATION = MOVIE_DATA_STAGE;

CREATE OR REPLACE TABLE movie_data (
  item_id INTEGER,
  movie_title STRING,
  release_date DATE,  -- OR FLOAT if in epoch/float format
  imdb_url STRING,
  primary_genre STRING,
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() 
);

CREATE OR REPLACE TABLE USERS_DATA (
  user_id INTEGER,
  age INTEGER,
  gender CHAR(1),  
  occupation STRING,
  zip_code STRING  
);

CREATE OR REPLACE TABLE USER_RATINGS (
  user_id INTEGER,
  item_id INTEGER,
  rating INTEGER,
  timestamp TIMESTAMP
);