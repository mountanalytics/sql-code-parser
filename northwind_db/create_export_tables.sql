/* Script to create all the Extract tables that are migrated to Azure DF using CSV files */

/* CATEGORIES */
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Categories_Extract ON
  
INSERT INTO MA_NorthWindDB.dbo.Categories_Extract(CategoryID, CategoryName, Description)
SELECT CategoryID, CategoryName, Description
FROM MA_NorthWindDB.dbo.Categories;

 
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Categories_Extract OFF 


/* CUSTOMERS */

INSERT INTO MA_NorthWindDB.dbo.Customers_Extract (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax
FROM MA_NorthWindDB.dbo.Customers;

  
/* EMPLOYEETERRITORIES */

INSERT INTO MA_NorthWindDB.dbo.EmployeeTerritories_Extract(EmployeeID, TerritoryID)
SELECT EmployeeID, TerritoryID
FROM MA_NorthWindDB.dbo.EmployeeTerritories;


/* EMPLOYEES */
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Employees_Extract ON
  
INSERT INTO MA_NorthWindDB.dbo.Employees_Extract (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, ReportsTo)
SELECT EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, ReportsTo
FROM MA_NorthWindDB.dbo.Employees;


SET IDENTITY_INSERT MA_NorthWindDB.dbo.Employees_Extract OFF 


/* ORDER_DETAILS 3x*/

/* Create (temporary) summary table, potentially replace with subquery  */
INSERT INTO MA_NorthWindDB.dbo.Order_Details_Order_AGGR (orderid, NrOfProducts, avg_order_unitprice, max_order_discount, min_order_discount, total_quantity)
select 
				orderid, 
				count(*) as NrOfProducts, 
				avg(unitprice) as avg_order_unitprice,
				max(Discount) as max_order_discount,  
				min(Discount) as min_order_discount, 
				sum(quantity) as total_quantity
			from 
				dbo.[Order Details] ode 
			group by 
				OrderID;


INSERT INTO MA_NorthWindDB.dbo.Order_Details_Product_AGGR (productid, product_quantity_year)
select 
				productid, 
				sum(quantity) as product_quantity_year
			from 
				dbo.[Order Details] ode 
			group by 
				productid;

INSERT INTO MA_NorthWindDB.dbo.Order_Details_Extract(OrderID, ProductID, UnitPrice, Quantity, Discount, NrOfProducts, avg_order_unitprice, max_order_discount, min_order_discount, total_quantity, product_quantity_year, 	perc_of_product_quantity_year)
SELECT 
	a.*, 
	c.NrOfProducts, 
	c.avg_order_unitprice,
	c.max_order_discount, 
	c.min_order_discount, 
	c.total_quantity,
	d.product_quantity_year, 
	round((cast(a.quantity as numeric (10,2))/cast(d.product_quantity_year as numeric (10,2)))*100,1) as perc_of_product_quantity_year
FROM 
	MA_NorthWindDB.dbo.[Order Details] a INNER JOIN MA_NorthWindDB.dbo.Orders b
	on a.OrderID = b.OrderID 
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Order_AGGR c
	on a.OrderID = c.OrderID	
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Product_AGGR d
	on a.productID = d.productID
where 
	YEAR(b.OrderDate) = 1996;


INSERT INTO MA_NorthWindDB.dbo.Order_Details_Extract(OrderID, ProductID, UnitPrice, Quantity, Discount, NrOfProducts, avg_order_unitprice, max_order_discount, min_order_discount, total_quantity, product_quantity_year, 	perc_of_product_quantity_year)
SELECT 
	a.*, 
	c.NrOfProducts, 
	c.avg_order_unitprice,
	c.max_order_discount, 
	c.min_order_discount, 
	c.total_quantity,
	d.product_quantity_year, 
	round((cast(a.quantity as numeric (10,2))/cast(d.product_quantity_year as numeric (10,2)))*100,1) as perc_of_product_quantity_year
FROM 
	MA_NorthWindDB.dbo.[Order Details] a INNER JOIN MA_NorthWindDB.dbo.Orders b
	on a.OrderID = b.OrderID 
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Order_AGGR c
	on a.OrderID = c.OrderID	
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Product_AGGR d
	on a.productID = d.productID
where 
	YEAR(b.OrderDate) = 1997;

INSERT INTO MA_NorthWindDB.dbo.Order_Details_Extract(OrderID, ProductID, UnitPrice, Quantity, Discount, NrOfProducts, avg_order_unitprice, max_order_discount, min_order_discount, total_quantity, product_quantity_year, 	perc_of_product_quantity_year)
SELECT 
	a.*, 
	c.NrOfProducts, 
	c.avg_order_unitprice,
	c.max_order_discount, 
	c.min_order_discount, 
	c.total_quantity,
	d.product_quantity_year, 
	round((cast(a.quantity as numeric (10,2))/cast(d.product_quantity_year as numeric (10,2)))*100,1) as perc_of_product_quantity_year
FROM 
	MA_NorthWindDB.dbo.[Order Details] a INNER JOIN MA_NorthWindDB.dbo.Orders b
	on a.OrderID = b.OrderID 
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Order_AGGR c
	on a.OrderID = c.OrderID	
	INNER JOIN MA_NorthWindDB.dbo.Order_Details_Product_AGGR d
	on a.productID = d.productID
where 
	YEAR(b.OrderDate) = 1998;


  



/* ORDERS 3x */
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract ON
  
INSERT INTO MA_NorthWindDB.dbo.Orders_Extract(OrderID, CustomerID, EmployeeID, OrderDate, OrderDate_Month, RequiredDate,Days_order_shipment, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry, ShipAddressConcat)
SELECT 
	OrderID, 
	CustomerID, 
	EmployeeID, 
	OrderDate, 
	CAST(DATEFROMPARTS(DATEPART(year, OrderDate), DATEPART(month, OrderDate),1) as DATETIME)as OrderDate_Month,
	RequiredDate,
	DATEDIFF(day, OrderDate, ShippedDate)as Days_order_shipment,
	ShippedDate, 
	ShipVia, 
	Freight, 
	ShipName, 
	ShipAddress, 
	ShipCity, 
	ShipRegion, 
	ShipPostalCode, 
	ShipCountry,
	CONCAT(ShipAddress, ' ', ShipCity, ' ', ShipRegion, ' ', ShipPostalCode, ' ', ShipCountry) as ShipAddressConcat
FROM 
	MA_NorthWindDB.dbo.Orders
where 
	YEAR(OrderDate) = 1996;

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract OFF



SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract ON
  
INSERT INTO MA_NorthWindDB.dbo.Orders_Extract(OrderID, CustomerID, EmployeeID, OrderDate, OrderDate_Month, RequiredDate,Days_order_shipment, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry, ShipAddressConcat)
SELECT 
	OrderID, 
	CustomerID, 
	EmployeeID, 
	OrderDate, 
	CAST(DATEFROMPARTS(DATEPART(year, OrderDate), DATEPART(month, OrderDate),1) as DATETIME)as OrderDate_Month,
	RequiredDate,
	DATEDIFF(day, OrderDate, ShippedDate)as Days_order_shipment,
	ShippedDate, 
	ShipVia, 
	Freight, 
	ShipName, 
	ShipAddress, 
	ShipCity, 
	ShipRegion, 
	ShipPostalCode, 
	ShipCountry,
	CONCAT(ShipAddress, ' ', ShipCity, ' ', ShipRegion, ' ', ShipPostalCode, ' ', ShipCountry) as ShipAddressConcat
FROM 
	MA_NorthWindDB.dbo.Orders
where 
	YEAR(OrderDate) = 1997;

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract OFF


SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract ON
  
INSERT INTO MA_NorthWindDB.dbo.Orders_Extract(OrderID, CustomerID, EmployeeID, OrderDate, OrderDate_Month, RequiredDate,Days_order_shipment, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry, ShipAddressConcat)
SELECT 
	OrderID, 
	CustomerID, 
	EmployeeID, 
	OrderDate, 
	CAST(DATEFROMPARTS(DATEPART(year, OrderDate), DATEPART(month, OrderDate),1) as DATETIME)as OrderDate_Month,
	RequiredDate,
	DATEDIFF(day, OrderDate, ShippedDate)as Days_order_shipment,
	ShippedDate, 
	ShipVia, 
	Freight, 
	ShipName, 
	ShipAddress, 
	ShipCity, 
	ShipRegion, 
	ShipPostalCode, 
	ShipCountry,
	CONCAT(ShipAddress, ' ', ShipCity, ' ', ShipRegion, ' ', ShipPostalCode, ' ', ShipCountry) as ShipAddressConcat
FROM 
	MA_NorthWindDB.dbo.Orders
where 
	YEAR(OrderDate) = 1998;

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Orders_Extract OFF  


  
/* PRODUCTS */
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Products_Extract ON  

INSERT INTO MA_NorthWindDB.dbo.Products_Extract (ProductID, ProductName, beverage,	SupplierID, CategoryID, QuantityPerUnit, UnitPrice, StockShortage, UnitsInStock, UnitsOnOrder, 	ReorderLevel,	Discontinued)
SELECT 
	ProductID, 
	ProductName,
	case 
		when QuantityPerUnit like '%bottles' THEN 'BEVERAGE'
		ELSE NULL END AS beverage,  
	SupplierID, 
	CategoryID, 
	QuantityPerUnit, 
	UnitPrice, 
	case 
		when UnitsInStock < UnitsOnOrder THEN CAST((UnitsInStock-UnitsOnOrder) as VARCHAR)
		WHEN UnitsOnOrder > 0 and UnitsInStock = UnitsOnOrder THEN 'No stock left'
		WHEN UnitsOnOrder > 0 and (UnitsInStock - UnitsOnOrder) < ReorderLevel THEN 'time to reorder'
		ELSE NULL END AS StockShortage, 
	UnitsInStock, 
	UnitsOnOrder, 
	ReorderLevel, 
	Discontinued
FROM 	
	MA_NorthWindDB.dbo.Products;

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Products_Extract OFF  

/* REGION */

INSERT INTO MA_NorthWindDB.dbo.Region_Extract(RegionID, RegionDescription)
SELECT RegionID, RegionDescription
FROM MA_NorthWindDB.dbo.Region;


/* SUPPLIERS */
SET IDENTITY_INSERT MA_NorthWindDB.dbo.Suppliers_Extract ON  
  
INSERT INTO MA_NorthWindDB.dbo.Suppliers_Extract ( SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
SELECT SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax
FROM MA_NorthWindDB.dbo.Suppliers;

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Suppliers_Extract OFF 
  
/* SHIPPERS */

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Shippers_Extract ON
GO
  
INSERT INTO MA_NorthWindDB.dbo.Shippers_Extract (ShipperID, CompanyName, Phone)
SELECT ShipperID, CompanyName, Phone
FROM MA_NorthWindDB.dbo.Shippers

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Shippers_Extract OFF 
GO


/* TERRITORIES*/
INSERT INTO MA_NorthWindDB.dbo.Territories_Extract (TerritoryID, TerritoryDescription, RegionID)
SELECT TerritoryID, trim(TerritoryDescription) as TerritoryDescription, RegionID
FROM MA_NorthWindDB.dbo.Territories;




