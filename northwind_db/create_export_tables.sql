/*
** Copyright Microsoft, Inc. 1994 - 2000
** All Rights Reserved.
*/

-- This script does not create a database.
-- Run this script in the database you want the objects to be created.
-- Default schema is dbo.

SET NOCOUNT ON
GO

set quoted_identifier on
GO

/* Set DATEFORMAT so that the date strings are interpreted correctly regardless of
   the default DATEFORMAT on the server.
*/
SET DATEFORMAT mdy
GO


/* 
  Drop the tables if they exist 
*/

if exists (select * from sysobjects where id = object_id('dbo.Order Details_1996_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order Details_1996_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Order Details_1997_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order Details_1997_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Order Details_1998_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order Details_1998_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Orders_1996_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1996_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Orders_1997_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1997_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Orders_1998_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1998_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Products_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Products_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Categories_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Categories_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Customers_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Customers_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Shippers_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Shippers_EXPORT"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Suppliers_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Suppliers_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.EmployeeTerritories_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."EmployeeTerritories_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Territories_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Territories_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Region_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Region_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Employees_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Employees_EXPORT"
GO  

CREATE TABLE "Employees_Export" (
	"EmployeeID" "int" IDENTITY (1, 1) NOT NULL ,
	"LastName" nvarchar (20) NOT NULL ,
	"FirstName" nvarchar (10) NOT NULL ,
	"Title" nvarchar (30) NULL ,
	"TitleOfCourtesy" nvarchar (25) NULL ,
	"BirthDate" "datetime" NULL ,
	"HireDate" "datetime" NULL ,
	"Address" nvarchar (60) NULL ,
	"City" nvarchar (15) NULL ,
	"Region" nvarchar (15) NULL ,
	"PostalCode" nvarchar (10) NULL ,
	"Country" nvarchar (15) NULL ,
	"HomePhone" nvarchar (24) NULL ,
	"Extension" nvarchar (4) NULL ,
	"ReportsTo" "int" NULL 
)
GO
