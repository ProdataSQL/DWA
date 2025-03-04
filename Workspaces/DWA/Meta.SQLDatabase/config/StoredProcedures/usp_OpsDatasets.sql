

/*

*/
CREATE     PROCEDURE [config].[usp_OpsDatasets]
AS
BEGIN
    SET NOCOUNT ON

	SELECT Dataset, Monitored, MaxAgeHours, PackageGroup, workspace
	FROM config.Datasets ds
	INNER JOIN Configurations c on c.ConfigurationID =ds.ConfigurationID
	CROSS APPLY OPENJSON (c.ConnectionSettings) WITH (
		workspace varchar(128)
	) j
	WHERE c.[Enabled]=1 
END

GO

