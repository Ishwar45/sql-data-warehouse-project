/*

************************************************************
CREATE DATABASE AND SCHEMAS
***********************************************************
These scripts include creating a database named 'datawarehouse' and along with that there are 3 schemas 'bronze', 'silver' and 'gold'.

WARNING:
Proceed with caution.
*/

use master;
GO

--Create a database named 'datawarehouse'
create database datawarehouse;
GO

 use datawarehouse;
 GO

 --Creating bronze, silver and gold schemas.
 CREATE SCHEMA bronze;
 GO

 CREATE SCHEMA silver; 
 GO

 CREATE SCHEMA gold;
 GO
