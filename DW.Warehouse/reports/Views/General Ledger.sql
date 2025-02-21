-- Auto Generated (Do not modify) A4F44FC40CF68B5A9C4888A0A20C9C54B3026E6598F2F6AF63937934C1F594D2




/* Description: AW General Ledger for PowerBI Direct Lake Model

   History: 
			21/02/2025 Created
*/
CREATE VIEW [reports].[General Ledger]
AS
SELECT DateKey
	  ,DepartmentGroupKey
	  ,ScenarioKey
	  ,OrganizationKey
	  ,CONVERT(MONEY, Amount * CASE WHEN a.AccountType IN ('Expenditures', 'Liabilities') THEN -1 ELSE 1 END) AS BaseAmount
	  ,f.AccountKey
	  ,Date 
FROM aw.FactFinance f 
INNER JOIN aw.DimAccount a 
		ON a.AccountKey = f.AccountKey