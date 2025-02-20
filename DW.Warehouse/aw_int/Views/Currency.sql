-- Auto Generated (Do not modify) 3ECCC8478CD6EC2DD8FE82A5A0E6D31D54C47E752B680C2872653DB38C86E2BE

/****** Object:  View [aw_int].[Currency]    Script Date: 06/11/2024 19:39:49 ******/

/* Description: AW DimCurrency
   Example: EXEC dwa.usp_TableLoad NULL,2,NULL
   History: 
			19/02/2025 Deepak Created
*/
CREATE VIEW [aw_int].[Currency]
AS
SELECT  CONVERT(VARCHAR(16),HASHBYTES('MD5', [CurrencyCode]),2) AS CurrencyKey
	, ISNULL(CONVERT(CHAR(3), [CurrencyCode]), '') AS CurrencyAlternateKey
	, ISNULL(CONVERT(VARCHAR(50), [CurrencyName]), '') AS CurrencyName
	, ROW_NUMBER() OVER (PARTITION BY   [CurrencyCode],[CurrencyName] ORDER BY [CurrencyCode] DESC) AS RowVersionNo
	, ISNULL(CONVERT(bigint,BINARY_CHECKSUM([CurrencyName],[CurrencyCode])),0) AS RowChecksum
	, CONVERT(VARCHAR(512), [FileName]) AS FileName
	,ISNULL(CONVERT(VARCHAR(36),LineageKey),0) AS LineageKey
FROM LH.aw_stg.[currency];