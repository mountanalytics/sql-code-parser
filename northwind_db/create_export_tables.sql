/* Script to create all the export tables that are migrated to Azure DF using CSV files */

SELECT CategoryID, CategoryName, Description
INSERT INTO MA_NorthWindDB.dbo.Categories_export
FROM MA_NorthWindDB.dbo.Categories;
GO
