
/*
Description:	Return a Queue of Pipelines to Run based on PipelineSequence
Used By:		ADF Pipeline-Worker 

Example
	exec config.[usp_PipelineQueue] 'AW'

History:	
	30/04/2024 Bob, Migrated to Fabric
	27/08/2024 Aidan, Implemented package group hierarchy
	29/08/2024 Bob, Added TemplateType for Dynamic Pipeline-Worker
	11/09/2024 Aidan, Ignores PackageGroup if PipelineID is specified
	31/09/2024 Kristan, added to config schema
	06/11/2024 Aidan, Added TableID 
*/
CREATE    PROC [config].[usp_PipelineQueue] 
	@PackageGroup [varchar](50) ='ALL'
	,@PipelineSequence [smallint] =-1
	,@PipelineID [int] =-1
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @ChecksumToken INT
	DECLARE @OldChecksumToken INT

	IF @PipelineID <> -1 AND @PipelineID IS NOT NULL
	BEGIN
		SELECT @PackageGroup='ALL',@PipelineSequence=-1
	END

	SELECT @PackageGroup=COALESCE(@PackageGroup, 'ALL'), @PipelineSequence=COALESCE(@PipelineSequence,-1)


	SELECT @OldChecksumToken=ChecksumToken FROM [config].[PipelineMetaCache]
	SELECT @ChecksumToken = CHECKSUM_AGG(checksum(*)) 
	FROM config.Pipelines p
	INNER JOIN config.PipelineGroups pg ON pg.PipelineGroupID =p.PipelineGroupID
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


	;WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup FROM string_split (@PackageGroup,',') WHERE value <> 'ALL'
		UNION ALL
        SELECT PackageGroup FROM config.PackageGroups pg WHERE  @PackageGroup='ALL'
		UNION 
		SELECT pgl.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value) 
		UNION 
		SELECT pgl2.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl  ON pgl.PackageGroup  = TRIM(pg2.value) 
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup =  pgl.ChildPackageGroup
	)
	SELECT NEWID() as LineageKey, [PipelineID], [PipelineSequence],[SourceConnectionSettings],[TargetConnectionSettings],[SourceSettings],[TargetSettings],[ActivitySettings],[PreExecuteSQL],[PostExecuteSQL],[Stage]
	, [Template], coalesce(TemplateType, Template) as TemplateType, TemplateID, p.TableID as TableID
	FROM [config].[PipelineMeta] p
	WHERE p.Enabled=1 
	AND (p.PackageGroup=@PackageGroup OR p.[PackageGroup] IN (SELECT PackageGroup FROM pg) or @PackageGroup ='ALL')
	AND (p.PipelineID=@PipelineID OR @PipelineID=-1)
	AND (p.PipelineSequence=@PipelineSequence OR @PipelineSequence =-1) 
	ORDER BY p.PipelineSequence, p.PipelineID 


END

GO

