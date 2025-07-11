/*
Description: Build PipelineMeta table 
Example:		
	exec [config].[usp_PipelineBuildMetaData]
History:
	11/11/2024	Kristan, removed ConfigurationValue
	11/11/2024	Kristan, fixed p and pg connections settings
	03/12/2024 	Kristan, removed TemplateType from config.Templates
	10/03/2024	Kristan, added SessionTag
	25/04/2025	Kristan, added LakehouseConnectionSettings
	29/06/2025  Bob, Added support for multiple workspaces for Templates
*/
CREATE PROC [config].[usp_PipelineBuildMetaData]
@PipelineID int =null
AS
BEGIN
	SET NOCOUNT ON;	
	DECLARE @Finished		bit=0
			,@OldPipelineID INT=0
			,@SinglePipeline bit=0

	if not exists (select * from config.Pipelines ) 
		return

	IF @PipelineID is not null
		BEGIN
			DELETE FROM [config].[PipelineMeta] WHERE PipelineID=@PipelineID
			SET @SinglePipeline=1
		END
	ELSE
		DELETE FROM [config].[PipelineMeta]

	IF coalesce(@PipelineID,0) =0 
		SELECT TOP 1 @PipelineID =PipelineID 
		FROM Pipelines 
		WHERE PipelineID > @OldPipelineID
	ORDER BY PipelineID

	SET @OldPipelineID =@PipelineID

	WHILE @Finished =0
	BEGIN
		DECLARE @SourceConnectionSettings VARCHAR(8000) = NULL
				,@SourceConnectionSettings1 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings2 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings3 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings4 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings1 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings2 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings3 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings4 VARCHAR(8000) = NULL
			   ,@SourceSettings1 VARCHAR(8000) = NULL
			   ,@SourceSettings2 VARCHAR(8000) = NULL
			   ,@SourceSettings3 VARCHAR(8000) = NULL
			   ,@SourceSettings VARCHAR(8000) = NULL
			   ,@TargetSettings1 VARCHAR(8000) = NULL
			   ,@TargetSettings2 VARCHAR(8000) = NULL
			   ,@TargetSettings3 VARCHAR(8000) = NULL
			   ,@TargetSettings VARCHAR(8000) = NULL
			   ,@Template VARCHAR(255) = NULL
			   ,@ActivitySettings VARCHAR(8000) = NULL
			   ,@ActivitySettings1 VARCHAR(8000) = NULL
			   ,@ActivitySettings2 VARCHAR(8000) = NULL
			   ,@PackageGroup VARCHAR(50) = NULL
			   ,@PreExecuteSQL VARCHAR(8000) = NULL
			   ,@PostExecuteSQL VARCHAR(8000) = NULL
			   ,@Enabled INT = NULL
			   ,@Stage VARCHAR(20) = NULL
			   ,@PipelineSequence SMALLINT = NULL
			   ,@SourceObject VARCHAR(512) = NULL
			   ,@TargetSchemaName VARCHAR(128) = NULL
			   ,@TargetTableName VARCHAR(128) = NULL
			   ,@TargetDirectory VARCHAR(512) = NULL
			   ,@TargetFileName VARCHAR(128) = NULL
			   ,@StartDateTime DATETIME2(6) = NULL
			   ,@ActivityType VARCHAR(10) = NULL
			   ,@Pipeline varchar(200) = NULL
			   ,@TemplateType varchar(50) = NULL
			   ,@TemplateWorkspace varchar(50) = NULL
			   ,@TableID int = NULL
			   ,@SessionTag varchar(50) = NULL
			   ,@ID UNIQUEIDENTIFIER = NULL

		SELECT	@Pipeline=p.Pipeline
			  ,@Template = COALESCE(p.Template, pg.Template)
			  ,@TargetConnectionSettings1 = p.TargetConnectionSettings
			  ,@TargetConnectionSettings2 = pg.TargetConnectionSettings
			  ,@TargetConnectionSettings3 = c2.ConnectionSettings
			  ,@TargetConnectionSettings4 = c4.ConnectionSettings
			  ,@SourceConnectionSettings1 = p.SourceConnectionSettings
			  ,@SourceConnectionSettings2 = pg.SourceConnectionSettings
			  ,@SourceConnectionSettings3 = c1.ConnectionSettings
			  ,@SourceConnectionSettings4 = c3.ConnectionSettings
			  ,@SourceSettings1 = nullif(p.SourceSettings,'')
			  ,@SourceSettings2 = nullif(pg.SourceSettings,'')
			  ,@SourceSettings3 = nullif(t.SourceSettings,'')
			  ,@TargetSettings1 = nullif(p.TargetSettings,'')
			  ,@TargetSettings2 = nullif(pg.TargetSettings,'')
			  ,@TargetSettings3 = nullif(t.TargetSettings,'')
			  ,@ActivitySettings1 = nullif(p.ActivitySettings,'')
			  ,@ActivitySettings2 = nullif(pg.ActivitySettings,'')
			  ,@PipelineSequence = p.PipelineSequence
			  ,@PackageGroup = COALESCE(p.PackageGroup, pg.PackageGroup)
			  ,@PreExecuteSQL = COALESCE(p.PreExecuteSQL, pg.PreExecuteSQL)
			  ,@PostExecuteSQL = COALESCE(p.PostExecuteSQL, pg.PostExecuteSQL)
			  ,@Enabled = CASE WHEN p.Enabled = 0 OR pg.Enabled = 0 THEN 0 ELSE 1 END
			  ,@Stage = COALESCE(pg.Stage, p.Stage)
			  ,@StartDateTime = GETDATE()
			  ,@ActivityType = CASE WHEN LEFT(LTRIM(COALESCE(p.ActivitySettings, pg.ActivitySettings)),1) = '{' THEN 'JSON' WHEN LEFT(LTRIM(COALESCE(p.ActivitySettings, pg.ActivitySettings)),1) = '<' THEN 'XML' ELSE 'OTHER' END
			  ,@TemplateType= t.ArtefactType
			  ,@ID=a.artefact_id
			  ,@TableID = p.TableID
			  ,@SessionTag = COALESCE(p.SessionTag, pg.SessionTag, 'ETL')
			  ,@TemplateWorkspace=a.workspace_id
			  FROM [Pipelines] p
		LEFT JOIN config.PipelineGroups pg ON pg.PipelineGroupID = p.PipelineGroupID
		LEFT JOIN config.[Templates] t on coalesce(p.[Template],pg.[Template]) =t.[Template]
		LEFT JOIN [config].[Configurations] c1 on pg.SourceConfigurationID = c1.ConfigurationID
		LEFT JOIN [config].[Configurations] c2 on pg.TargetConfigurationID = c2.ConfigurationID
		LEFT JOIN [config].[Configurations] c3 on p.SourceConfigurationID = c3.ConfigurationID
		LEFT JOIN [config].[Configurations] c4 on p.TargetConfigurationID = c4.ConfigurationID
		LEFT JOIN config.Artefacts a on t.ArtefactType in ('Notebook','DataPipeline') and t.Template = a.artefact
		WHERE p.PipelineID = @PipelineID

		--DEBUG
		--SELECT @Pipeline AS Pipeline,@Template AS Template,@SourceConnectionSettings1 AS SourceConnectionSettings1,@SourceConnectionSettings2 AS SourceConnectionSettings2,@SourceConnectionSettings3 AS SourceConnectionSettings3,@SourceConnectionSettings4 AS SourceConnectionSettings4,@TargetConnectionSettings1 AS TargetConnectionSettings1,@TargetConnectionSettings2 AS TargetConnectionSettings2,@TargetConnectionSettings3 AS TargetConnectionSettings3,@TargetConnectionSettings4 AS TargetConnectionSettings4,@SourceSettings1 AS SourceSettings1,@SourceSettings2 AS SourceSettings2,@SourceSettings3 AS SourceSettings3,@TargetSettings1 AS TargetSettings1,@TargetSettings2 AS TargetSettings2,@TargetSettings3 AS TargetSettings3,@ActivitySettings1 AS ActivitySettings1,@ActivitySettings2 AS ActivitySettings2,@PipelineSequence AS PipelineSequence,@PackageGroup AS PackageGroup,@PreExecuteSQL AS PreExecuteSQL,@PostExecuteSQL AS PostExecuteSQL,@Enabled AS Enabled,@Stage AS Stage,@StartDateTime AS StartDateTime,@ActivityType AS ActivityType,@TemplateType AS TemplateType,@ID AS ArtefactID, @TableID as TableID, @SessionTag as SessionTag, @LakehouseConnectionSettings as LakehouseConnectionSettings
		
		PRINT  @PipelineID

		IF @ActivityType = 'JSON'
			BEGIN 
				;WITH AcS1 AS (SELECT * FROM OPENJSON(@ActivitySettings1)),
					  AcS2 AS (SELECT * FROM OPENJSON(@ActivitySettings2))
				 SELECT @ActivitySettings = (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM 
											(
												SELECT * FROM AcS1
												UNION ALL
												SELECT * FROM AcS2
												WHERE [key] NOT IN (SELECT [key] FROM AcS1)
											) s)
			END
		ELSE 
			SET @ActivitySettings = COALESCE(@ActivitySettings1, @ActivitySettings2)
		
		;WITH SS1 AS (SELECT * FROM OPENJSON(@SourceSettings1)),
			  SS2 AS (SELECT * FROM OPENJSON(@SourceSettings2)),
			  SCS1 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings1)),
			  SCS2 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings2)),
			  SCS3 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings3)),
			  SCS4 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings4)),
			  TS1 AS (SELECT * FROM OPENJSON(@TargetSettings1)),
			  TS2 AS (SELECT * FROM OPENJSON(@TargetSettings2)),
			  TCS1 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings1)),
			  TCS2 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings2)),
			  TCS3 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings3)),
			  TCS4 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings4))
		SELECT	@SourceConnectionSettings =  (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM (
                    SELECT * FROM SCS1
					UNION ALL
					SELECT * FROM SCS4
					WHERE EXISTS (SELECT 1 FROM SCS4)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)

					-- If SCS4 does not exist, follow the SCS3, SCS2, SCS1 priority
					UNION ALL
					SELECT * FROM SCS3
					WHERE NOT EXISTS (SELECT 1 FROM SCS4)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)
          
					UNION ALL
					SELECT * FROM SCS2
					WHERE NOT EXISTS (SELECT 1 FROM SCS4)
					  AND NOT EXISTS (SELECT 1 FROM SCS3)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)
				) s),
				@SourceSettings = (
				SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM (
	 					SELECT * FROM SS1
	 					UNION ALL
	 					SELECT * FROM SS2
	 					WHERE [key] NOT IN (SELECT [key] FROM SS1)
	 					UNION ALL
	 					SELECT * FROM OPENJSON(@SourceSettings3)
	 					WHERE [key] NOT IN (SELECT [key] FROM SS1)
	 						AND [key] NOT IN (SELECT [key] FROM SS2)
	 			) s ),
				@TargetConnectionSettings = (
				SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM (
	 					SELECT * FROM TCS1
						UNION ALL
						SELECT * FROM TCS4
						WHERE EXISTS (SELECT 1 FROM TCS4)
							AND [key] NOT IN (SELECT [key] FROM TCS1)

						UNION ALL
						SELECT * FROM TCS3
						WHERE NOT EXISTS (SELECT 1 FROM TCS4)
							AND [key] NOT IN (SELECT [key] FROM TCS1)

						UNION ALL
						SELECT * FROM TCS2
						WHERE NOT EXISTS (SELECT 1 FROM TCS4)
							AND NOT EXISTS (SELECT 1 FROM TCS3)
							AND [key] NOT IN (SELECT [key] FROM TCS1)
				) s),
				@TargetSettings = (
				SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM (
						SELECT * FROM TS1
						UNION ALL
						SELECT * FROM TS2
						WHERE [key] NOT IN (SELECT [key] FROM TS1)
						UNION ALL 
						SELECT * FROM OPENJSON(@TargetSettings3)
						WHERE [key] NOT IN (SELECT [key] FROM TS1)
							AND [key] NOT IN (SELECT [key] FROM TS2)
				) s)			
		IF NOT EXISTS (SELECT * FROM [config].[PipelineMeta] WHERE PipelineID=@PipelineID)
			INSERT INTO [config].[PipelineMeta]	([PipelineID], [Pipeline], [Template], [SourceConnectionSettings], [TargetConnectionSettings], [SourceSettings], [TargetSettings], [ActivitySettings], [PipelineSequence], [PackageGroup], [PreExecuteSQL], [PostExecuteSQL], [Enabled], [Stage],TemplateType, TemplateID,TableID,SessionTag,TemplateWorkspaceID)
			VALUES (@PipelineID, @Pipeline,@Template, @SourceConnectionSettings, @TargetConnectionSettings, @SourceSettings, @TargetSettings, @ActivitySettings, @PipelineSequence, @PackageGroup, @PreExecuteSQL, @PostExecuteSQL, @Enabled, @Stage,@TemplateType,@ID, @TableID,@SessionTag, @TemplateWorkspace)
	
		SELECT TOP 1 @PipelineID =PipelineID 
		FROM Pipelines 
		WHERE PipelineID > @OldPipelineID
		ORDER BY PipelineID

		IF @PipelineID is null OR @PipelineID=@OldPipelineID OR @SinglePipeline=1
			SET @Finished=1
		ELSE
			SET @OldPipelineID =@PipelineID
	END
END

GO

