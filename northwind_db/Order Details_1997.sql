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
	INNER JOIN 
		(	
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
				OrderID
		) c
	on a.OrderID = c.OrderID	
	INNER JOIN 
		(
			select 
				productID, 
				sum(quantity) as product_quantity_year
			from 
				dbo.[Order Details] ode 
			group by 
				productID
		) d
	on a.productID = d.productID
where 
	YEAR(b.OrderDate) = 1997;
