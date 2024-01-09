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