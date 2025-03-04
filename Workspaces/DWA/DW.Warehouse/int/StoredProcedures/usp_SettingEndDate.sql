/* 
	Description: Set EndDate for populating DimDate on the basis on max date from Trans table     
	History:
		20/02/2025  Created                
*/
CREATE PROC [int].[usp_SettingEndDate] AS
BEGIN
    SET NOCOUNT ON;
   
    DECLARE @EndDate AS INT;
	DECLARE @MaxDate AS DATE;

	SET @MaxDate = (
			SELECT COALESCE(MAX(Date),getdate())
			FROM tst.Finance
			);
	SET @EndDate = (
			SELECT CONVERT(VARCHAR(8), DATEADD(yy, DATEDIFF(yy, 0, MAX(DATEADD(d, 7, CONVERT(DATE, @MaxDate)))) + 1, - 1), 112) AS EndDate
			);

	UPDATE config.Settings
	SET [Value] = @EndDate
	WHERE Entity = 'EDW'
		AND Attribute = 'EndDate';
END