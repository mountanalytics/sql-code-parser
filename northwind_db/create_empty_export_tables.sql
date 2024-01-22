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

if exists (select * from sysobjects where id = object_id('dbo.Order_Details_Order_AGGR') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_Order_AGGR"
GO

if exists (select * from sysobjects where id = object_id('dbo.Order_Details_Product_AGGR') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_Product_AGGR"
GO
	
if exists (select * from sysobjects where id = object_id('dbo.Order_Details_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_Extract"
GO

if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1996_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1996_Extract"
GO
	
if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1997_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1997_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Order_Details_1998_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Order_Details_1998_Extract"
GO

if exists (select * from sysobjects where id = object_id('dbo.Orders_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Orders_1996_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1996_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Orders_1997_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1997_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Orders_1998_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Orders_1998_Extract"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Products_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Products_Extract"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Categories_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Categories_Extract"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Customers_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Customers_Extract"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Shippers_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Shippers_Extract"
GO
  
if exists (select * from sysobjects where id = object_id('dbo.Suppliers_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Suppliers_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.EmployeeTerritories_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."EmployeeTerritories_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Territories_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Territories_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Region_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Region_Extract"
GO
if exists (select * from sysobjects where id = object_id('dbo.Employees_Extract') and sysstat & 0xf = 3)
	drop table "dbo"."Employees_Extract"
GO  

CREATE TABLE "Employees_Extract" (
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


CREATE TABLE "Categories_Extract" (
	"CategoryID" "int" IDENTITY (1, 1) NOT NULL ,
	"CategoryName" nvarchar (15) NOT NULL ,
	"Description" "ntext" NULL 
)


CREATE TABLE "Customers_Extract" (
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


CREATE TABLE "Shippers_Extract" (
	"ShipperID" "int" IDENTITY (1, 1) NOT NULL ,
	"CompanyName" nvarchar (40) NOT NULL ,
	"Phone" nvarchar (24) NULL 
)
GO

CREATE TABLE "Suppliers_Extract" (
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

CREATE TABLE "Orders_Extract" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDate_Month" "datetime" NULL ,
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

CREATE TABLE "Orders_1996_Extract" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDate_Month" "datetime" NULL ,
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

	CREATE TABLE "Orders_1997_Extract" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDate_Month" "datetime" NULL ,
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
	CREATE TABLE "Orders_1998_Extract" (
	"OrderID" "int" IDENTITY (1, 1) NOT NULL ,
	"CustomerID" nchar (5) NULL ,
	"EmployeeID" "int" NULL ,
	"OrderDate" "datetime" NULL ,
	"OrderDate_Month" "datetime" NULL ,
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
CREATE TABLE "Products_Extract" (
	"ProductID" "int" IDENTITY (1, 1) NOT NULL ,
	"ProductName" nvarchar (40) NOT NULL ,
	"beverage" nvarchar (40) NULL ,
	"SupplierID" "int" NULL ,
	"CategoryID" "int" NULL ,
	"QuantityPerUnit" nvarchar (20) NULL ,
	"UnitPrice" "money" NULL ,
	"StockShortage" nvarchar (40) NULL ,
	"UnitsInStock" "smallint" NULL ,
	"UnitsOnOrder" "smallint" NULL ,
	"ReorderLevel" "smallint" NULL ,
	"Discontinued" "bit" 
)
GO


CREATE TABLE "Order_Details_Order_AGGR" (
	"OrderID" "int" NOT NULL ,
	"NrOfProducts" "int" NOT NULL ,
	"avg_order_unitprice" "money" NOT NULL ,
	"max_order_discount" "real" NOT NULL ,
	"min_order_discount" "real" NOT NULL ,
	"total_quantity" "smallint" NOT NULL 
	)
GO


CREATE TABLE "Order_Details_Product_AGGR" (
	"ProductID" "int" NOT NULL ,
	"product_quantity_year" "smallint" NOT NULL 
	)
GO

CREATE TABLE "Order_Details_Extract" (
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
	
CREATE TABLE "Order_Details_1996_Extract" (
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


CREATE TABLE "Order_Details_1997_Extract" (
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


CREATE TABLE "Order_Details_1998_Extract" (
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



CREATE TABLE "Region_Extract" ( 
	"RegionID" "int" NOT NULL ,
	"RegionDescription" "nchar" (50) NOT NULL
) 
GO

CREATE TABLE "Territories_Extract" (
	"TerritoryID" "nvarchar" (20) NOT NULL ,
	"TerritoryDescription" "nchar" (50) NOT NULL ,
        "RegionID" [int] NOT NULL
) 
GO

CREATE TABLE "EmployeeTerritories_Extract" (
	"EmployeeID" "int" NOT NULL,
	"TerritoryID" "nvarchar" (20) NOT NULL
) 
GO
