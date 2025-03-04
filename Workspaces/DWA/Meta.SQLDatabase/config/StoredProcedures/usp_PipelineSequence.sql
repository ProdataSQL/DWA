







/*
Description:	Retrieve Array of PipelineOrder for Which Pipelines to run in which sequence
Used By:		ADF Pipeline-Controller 

-- Example
	exec [config].[usp_PipelineSequence] 'AW','ALL'
	exec [config].[usp_PipelineSequence] 'test-copy-blob'
	exec [config].[usp_PipelineSequence] 'GAMDAY0','Ingest'
	   	 
History:	
	08/05/2024 Bob, Migrated to Fabric 
	27/08/2024 Aidan, Implemented package group hierarchy
	29/08/2024 Bob, Trigger build of meta data cache if needed
	01/11/2024 Kristan, Added PackageGroup coalesce for PipelineGroups
	11/11/2024 Aiddan, Changed join to Pipelinegroups to a Left join

*/
CREATE   PROC [config].[usp_PipelineSequence] 
	@PackageGroup [varchar](4000) ='ALL'
	,@Stage [varchar](50) =null
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @ChecksumToken INT
	DECLARE @OldChecksumToken INT

	SELECT @OldChecksumToken=ChecksumToken FROM [config].[PipelineMetaCache]
	SELECT @ChecksumToken = CHECKSUM_AGG(checksum(*)) 
	FROM config.Pipelines p
	LEFT JOIN config.PipelineGroups pg ON pg.PipelineGroupID =p.PipelineGroupID
	LEFT JOIN config.Configurations c on pg.TargetConfigurationID =c.ConfigurationID
	LEFT JOIN config.Configurations c2 on pg.SourceConfigurationID =c2.ConfigurationID
		
	IF @OldChecksumToken <> @ChecksumToken or @OldChecksumToken is null
	BEGIN
		PRINT 'Rebuilding Cache. exec [config].[usp_PipelineBuildMetaData]'
		exec [config].[usp_PipelineBuildMetaData]

		IF EXISTS (SELECT * FROM [config].[PipelineMetaCache] )
			UPDATE [config].[PipelineMetaCache] SET ChecksumToken=@ChecksumToken
		ELSE
			INSERT INTO [config].[PipelineMetaCache] VALUES (@ChecksumToken)	
	END

	SELECT @PackageGroup =coalesce(@PackageGroup,'ALL')
	,@Stage =coalesce(NULLIF(@Stage,''),'ALL')
	
	;WITH pg as 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup FROM string_split (@PackageGroup,',') WHERE value <> 'ALL'
		UNION ALL
        SELECT PackageGroup FROM config.PackageGroups pg WHERE @PackageGroup='ALL'
		UNION 
		SELECT pgl.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value) 
		UNION 
		SELECT pgl2.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl  ON pgl.PackageGroup  = TRIM(pg2.value) 
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup =  pgl.ChildPackageGroup
	),
	s as 
	(
		SELECT value as Stage FROM string_split (@Stage,',') WHERE value <> 'ALL'
	)
	SELECT p.[PipelineSequence],max(coalesce(p.Stage, pg.Stage)) as Stage
	, convert(bit,max(coalesce(p.ContinueOnError, pg.ContinueOnError, 0 ))) as ContinueOnError
	FROM [config].[Pipelines] p
	LEFT JOIN config.PipelineGroups pg on pg.PipelineGroupID =p.PipelineGroupID
	WHERE coalesce(p.[PackageGroup],pg.[PackageGroup]) IN (SELECT PackageGroup FROM pg) 
	AND (coalesce(p.Stage,pg.Stage )  IN (SELECT Stage FROM s) OR @Stage ='ALL')
	GROUP BY p.PipelineSequence
	ORDER BY p.PipelineSequence

END

GO

