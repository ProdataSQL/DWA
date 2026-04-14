-- Auto Generated (Do not modify) 5CA9A03EB65C3360668826CD370CA490F22F0F186A83F1860FAA2300871EA2E6
/* Description: AW DimDepartmentGroup for PowerBI Model

   History: 
			26/10/2023 Shruti, ALTERd
*/
CREATE   VIEW [reports].[FactFinance]
AS
SELECT DateKey
	  ,DepartmentGroupKey
	  ,ScenarioKey
	  ,OrganizationKey
	  ,CONVERT(DECIMAL(18,2), Amount * CASE WHEN a.AccountType IN ('Expenditures', 'Liabilities') THEN -1 ELSE 1 END) AS BaseAmount
	  ,f.AccountKey
	  ,Date 
FROM aw.FactFinance f 
INNER JOIN aw.DimAccount a 
		ON a.AccountKey = f.AccountKey