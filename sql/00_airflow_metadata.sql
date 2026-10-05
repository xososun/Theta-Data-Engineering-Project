-- Create Airflow metadata database
-- This script runs as the 'postgres' superuser via PostgreSQL's docker-entrypoint-initdb.d mechanism
CREATE DATABASE airflow_metadata;
