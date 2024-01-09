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