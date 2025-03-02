/*
=============================================================
Create Database and Schemas in PostgreSQL
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'datawarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

-- Connect to the default database (PostgreSQL does not support 'USE')
-- Instead, you must run the script in the `postgres` database or a different admin database.

-- Drop database if it exists
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'datawarehouse';

DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- Connect to the newly created database
\c datawarehouse

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
