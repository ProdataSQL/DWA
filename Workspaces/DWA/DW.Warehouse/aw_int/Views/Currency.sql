-- Auto Generated (Do not modify) 42EDCCC8E2E054E244CDDC66742F402D23BF7C9A6439CCE2F097DA53FCA99584


/****** Object:  View [aw_int].[Currency]    Script Date: 06/11/2024 19:39:49 ******/

/* Description: AW DimCurrency
   Example: EXEC dwa.usp_TableLoad NULL,2,NULL
   History: 
			19/02/2025 Created
*/
Create   VIEW [aw_int].[Currency]
AS
SELECT  ISNULL(CONVERT(CHAR(3), [CurrencyCode]), '') AS CurrencyAlternateKey
	, ISNULL(CONVERT(VARCHAR(50), [CurrencyName]), '') AS CurrencyName
	, CONVERT(VARCHAR(512), [FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
FROM LH.aw_stg.[Currency];