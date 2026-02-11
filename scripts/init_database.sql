/*
================================================================
Create Database and Schemas
================================================================
Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. If it exists, it will be dropped and recreated. Additionally, it will create 3 schemas, bronze, silver and gold. 

WARNING:
 Running this will will drop the entire 'DataWarehouse' DB and all the data in the DB will be permanently deleted. 
*/


USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

--Create the DataWarehousedatabase
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
