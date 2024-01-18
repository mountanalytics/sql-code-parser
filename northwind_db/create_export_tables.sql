/* Script to create all the export tables that are migrated to Azure DF using CSV files */


SET IDENTITY_INSERT MA_NorthWindDB.dbo.Shippers_Export ON

INSERT INTO MA_NorthWindDB.dbo.Shippers_Export (ShipperID, CompanyName, Phone)
SELECT ShipperID, CompanyName, Phone
FROM MA_NorthWindDB.dbo.Shippers

SET IDENTITY_INSERT MA_NorthWindDB.dbo.Shippers_Export OFF 
GO
