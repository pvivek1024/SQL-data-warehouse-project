/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse'.
    The script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.
	*/

use master;
go

--CREATE DATABASE DATAWAREHOUSE--
create database datawarehouse;
go
--create Schemas  
use Datawarehouse;
go
  
create schema bronze;
go
create schema silver;
go   
create schema gold;
go
