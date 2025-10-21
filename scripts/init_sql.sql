/*
==========================================================
   Create Database and Schemas
==========================================================

Script Purpose:
   This script creates a new database named 'DataWarehouse' 
   after checking if it already exists.
   If the database exists, it is dropped and recreated. 
   Additionally, the script sets up three schemas within the database:
   'bronze', 'silver', and 'gold'.

WARNING:
   Running this script will drop the entire 'DataWarehouse' database 
   if it exists.
   ⚠️ All data in the database will be permanently deleted.
   Make sure you have proper backups before running this script.
==========================================================
*/

-- 🔹 Step 1 : Use the master database to perform admin operations
USE master;
GO

-- 🔹 Step 2 : Check if the database 'DataWarehouse' already exists
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    -- Force the database into SINGLE_USER mode (to disconnect users)
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;

    -- Drop the existing database
    DROP DATABASE DataWarehouse;
END;
GO

-- 🔹 Step 3 : Create a fresh 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

-- 🔹 Step 4 : Switch context to the new database
USE DataWarehouse;
GO

-- 🔹 Step 5 : Create three schemas for data organization
CREATE SCHEMA bronze;  -- For raw data (ingestion layer)
GO

CREATE SCHEMA silver;  -- For cleaned and transformed data
GO

CREATE SCHEMA gold;    -- For aggregated or analytical data
GO

