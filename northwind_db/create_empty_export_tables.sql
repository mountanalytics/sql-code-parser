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

if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1996_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1996_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1997_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1997_EXPORT"
GO
if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1998_EXPORT') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1998_EXPORT"
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


CREATE TABLE "Categories_Export" (
	"CategoryID" "int" IDENTITY (1, 1) NOT NULL ,
	"CategoryName" nvarchar (15) NOT NULL ,
	"Description" "ntext" NULL 
)


CREATE TABLE "Customers_Export" (
	"CustomerID" nchar (5) NOT NULL ,
	"CompanyName" nvarchar (40) NOT NULL ,
	"ContactName" nvarchar (30) NULL ,
	"ContactTitle" nvarchar (30) NULL ,
	"Address" nvarchar (60) NULL ,
	"City" nvarchar (15) NULL ,
	"Region" nvarchar (15) NULL ,
	"PostalCode" nvarchar (10) NULL ,
	"Country" nvarchar (15) NULL ,
	"Phone" nvarchar (24) NULL ,
	"Fax" nvarchar (24) NULL 
)
GO


CREATE TABLE "Shippers_Export" (
	"ShipperID" "int" IDENTITY (1, 1) NOT NULL ,
	"CompanyName" nvarchar (40) NOT NULL ,
	"Phone" nvarchar (24) NULL 
)
GO

CREATE TABLE "Suppliers_Export" (
	"SupplierID" "int" IDENTITY (1, 1) NOT NULL ,
	"CompanyName" nvarchar (40) NOT NULL ,
	"ContactName" nvarchar (30) NULL ,
	"ContactTitle" nvarchar (30) NULL ,
	"Address" nvarchar (60) NULL ,
	"City" nvarchar (15) NULL ,
	"Region" nvarchar (15) NULL ,
	"PostalCode" nvarchar (10) NULL ,
	"Country" nvarchar (15) NULL ,
	"Phone" nvarchar (24) NULL ,
	"Fax" nvarchar (24) NULL 
)
GO



CREATE TABLE "Orders_1996_Export" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDateMonth" "datetime" NULL ,
	"RequiredDate" "datetime" NULL ,
	"Days_order_shipment" "int" NULL ,
	"ShippedDate" "datetime" NULL ,
	"ShipVia" "int" NULL ,
	"Freight" "money" NULL ,
	"ShipName" nvarchar (40) NULL ,
	"ShipAddress" nvarchar (60) NULL ,
	"ShipCity" nvarchar (15) NULL ,
	"ShipRegion" nvarchar (15) NULL ,
	"ShipPostalCode" nvarchar (10) NULL ,
	"ShipCountry" nvarchar (15) NULL,
	"ShipAddressConcat" nvarchar (120) NULL 
)
GO

	CREATE TABLE "Orders_1997_Export" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDateMonth" "datetime" NULL ,
	"RequiredDate" "datetime" NULL ,
	"Days_order_shipment" "int" NULL ,
	"ShippedDate" "datetime" NULL ,
	"ShipVia" "int" NULL ,
	"Freight" "money" NULL ,
	"ShipName" nvarchar (40) NULL ,
	"ShipAddress" nvarchar (60) NULL ,
	"ShipCity" nvarchar (15) NULL ,
	"ShipRegion" nvarchar (15) NULL ,
	"ShipPostalCode" nvarchar (10) NULL ,
	"ShipCountry" nvarchar (15) NULL,
	"ShipAddressConcat" nvarchar (120) NULL 
)
GO

	
GO
	CREATE TABLE "Orders_1998_Export" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDateMonth" "datetime" NULL ,
	"RequiredDate" "datetime" NULL ,
	"Days_order_shipment" "int" NULL ,
	"ShippedDate" "datetime" NULL ,
	"ShipVia" "int" NULL ,
	"Freight" "money" NULL ,
	"ShipName" nvarchar (40) NULL ,
	"ShipAddress" nvarchar (60) NULL ,
	"ShipCity" nvarchar (15) NULL ,
	"ShipRegion" nvarchar (15) NULL ,
	"ShipPostalCode" nvarchar (10) NULL ,
	"ShipCountry" nvarchar (15) NULL,
	"ShipAddressConcat" nvarchar (120) NULL 
)
	
GO
CREATE TABLE "Products_Export" (
	"ProductID" "int" IDENTITY (1, 1) NOT NULL ,
	"ProductName" nvarchar (40) NOT NULL ,
	"beverage" nvarchar (40) NOT NULL ,
	"SupplierID" "int" NULL ,
	"CategoryID" "int" NULL ,
	"QuantityPerUnit" nvarchar (20) NULL ,
	"UnitPrice" "money" NULL ,
	"StockShortage" nvarchar (40) NOT NULL ,
	"UnitsInStock" "smallint" NULL ,
	"UnitsOnOrder" "smallint" NULL ,
	"ReorderLevel" "smallint" NULL ,
	"Discontinued" "bit" 
)
GO


CREATE TABLE "Order_Details_1996_Export" (
	"OrderID" "int" NOT NULL ,
	"ProductID" "int" NOT NULL ,
	"UnitPrice" "money" NOT NULL ,
	"Quantity" "smallint" NOT NULL ,
	"Discount" "real" NOT NULL ,
	"NrOfProducts" "int" NOT NULL ,
	"avg_order_unitprice" "money" NOT NULL ,
	"max_order_discount" "real" NOT NULL ,
	"min_order_discount" "real" NOT NULL ,
	"total_quantity" "smallint" NOT NULL ,
	"product_quantity_year" "smallint" NOT NULL ,
	"perc_of_product_quantity_year" "real" NOT NULL
)
GO


CREATE TABLE "Order_Details_1997_Export" (
	"OrderID" "int" NOT NULL ,
	"ProductID" "int" NOT NULL ,
	"UnitPrice" "money" NOT NULL ,
	"Quantity" "smallint" NOT NULL ,
	"Discount" "real" NOT NULL ,
	"NrOfProducts" "int" NOT NULL ,
	"avg_order_unitprice" "money" NOT NULL ,
	"max_order_discount" "real" NOT NULL ,
	"min_order_discount" "real" NOT NULL ,
	"total_quantity" "smallint" NOT NULL ,
	"product_quantity_year" "smallint" NOT NULL ,
	"perc_of_product_quantity_year" "real" NOT NULL
)
GO


CREATE TABLE "Order_Details_1998_Export" (
	"OrderID" "int" NOT NULL ,
	"ProductID" "int" NOT NULL ,
	"UnitPrice" "money" NOT NULL ,
	"Quantity" "smallint" NOT NULL ,
	"Discount" "real" NOT NULL ,
	"NrOfProducts" "int" NOT NULL ,
	"avg_order_unitprice" "money" NOT NULL ,
	"max_order_discount" "real" NOT NULL ,
	"min_order_discount" "real" NOT NULL ,
	"total_quantity" "smallint" NOT NULL ,
	"product_quantity_year" "smallint" NOT NULL ,
	"perc_of_product_quantity_year" "real" NOT NULL
)
GO



CREATE TABLE "Region_Export" ( 
	"RegionID" "int" NOT NULL ,
	"RegionDescription" "nchar" (50) NOT NULL
) 
GO

CREATE TABLE "Territories_Export" (
	"TerritoryID" "nvarchar" (20) NOT NULL ,
	"TerritoryDescription" "nchar" (50) NOT NULL ,
        "RegionID" [int] NOT NULL
) 
GO

CREATE TABLE "EmployeeTerritories_Export" (
	"EmployeeID" "int" NOT NULL,
	"TerritoryID" "nvarchar" (20) NOT NULL
) 
GO
