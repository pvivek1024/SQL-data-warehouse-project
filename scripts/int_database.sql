--CREATE DATABASE DATAWAREHOUSE--

use master;

create database datawarehouse;

use Datawarehouse;

create schema bronze;
go
create schema silver;
go   
create schema gold;
