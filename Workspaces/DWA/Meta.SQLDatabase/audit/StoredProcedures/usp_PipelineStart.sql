



/*
Description:	Start a Pipeline AND return all Meta Data (complex)
Used By:		ADF Pipeline-Worker 

-- Example
	exec [audit].[usp_PipelineStart]  37, '7EEFA88E-D9F9-4204-AC47-97F2FC3362CE', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82'
	SELECT * FROM audit.PipelineLog ORDER BY StartDateTime DESC 
	SELECT * FROM audit.LineageLog ORDER BY LineageKey DESC 
	exec [audit].[usp_PipelineStart] 37,  'cfb66af5-4815-4664-a494-f08fdcf9e0ab'
History:	
	24/11/2024 Bob, Migrated to Fabric Databaase

*/
CREATE  PROC [audit].[usp_PipelineStart]  
	@PipelineID [int] 
	,@LineageKey uniqueidentifier = null 
	,@RunID uniqueidentifier = null 
	,@ParentRunID uniqueidentifier = null
	,@WorkspaceID uniqueidentifier = null
	,@Pipeline uniqueidentifier = null
	,@PipelineName varchar(512) = null
	,@PackageGroup varchar(512) =null
AS
BEGIN

	DECLARE  @SourceConnectionSettings	varchar(8000)
			,@TargetConnectionSettings	varchar(8000)
			,@SourceSettings			varchar(8000)
			,@TargetSettings				varchar(8000)
			,@ActivitySettings			varchar(8000)
			,@PreExecuteSQL				varchar(8000)
			,@PostExecuteSQL			varchar(8000)
			,@Template				varchar(8000)
			,@StartDateTime				datetime2 = getdate()
			,@Stage						varchar(50)
			,@PipelineSequence			int
			,@MonitoringUrl				varchar(512)
			,@RetryCount				int = 0
	        ,@MaxRetryCount			    int = 3
			,@Success					BIT = 0
			,@TableID					int = null


	SET @LineageKey=COALESCE(@LineageKey,  NEWID())
	SELECT @SourceConnectionSettings=SourceConnectionSettings
	, @TargetConnectionSettings=TargetConnectionSettings
	, @SourceSettings=SourceSettings
	, @TargetSettings=TargetSettings
	, @ActivitySettings=ActivitySettings
	, @PreExecuteSQL=PreExecuteSQL
	, @PostExecuteSQL=PostExecuteSQL
	, @Template=Template
	, @Stage=Stage
	,@MonitoringUrl = lower('https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces/' + CONVERT(varchar(36), @WorkspaceID) + '/pipelines/' + @PipelineName + '/' + CONVERT(varchar(36), @RunID) )
	,@TableID = TableID
	FROM [config].[PipelineMeta]
	WHERE PipelineID=@PipelineID
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 			
			INSERT INTO [audit].[PipelineLog]	(
				 [LineageKey]
				, [RunID]
				, [ParentRunID]
				, [PipelineID]
				, WorkspaceID
				, [PackageGroup]
				, [Stage]
				, [StartDate]
				, [StartDateTime]
				, [Status]
				, PipelineSequence
				, [Template]
				, Pipeline
				, PipelineName
				, MonitoringUrl
				)

			VALUES (@LineageKey
				  ,@RunID 
				  ,@ParentRunID 
				  ,@PipelineID 
				  ,@WorkspaceID
				  ,@PackageGroup
				  ,@Stage 
				  ,CONVERT(DATE, @StartDateTime)
				  ,@StartDateTime
				  ,'Started'
				  ,@PipelineSequence 
				  ,@Template 
				  ,@Pipeline
				  ,@PipelineName
				  ,@MonitoringUrl)
			 SET @Success = 1
			END TRY
			BEGIN CATCH
			 IF ERROR_NUMBER() IN (24556) /* Update Concurrency Error */
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					DECLARE @BackoffDelay varchar(12) = RIGHT('00:00:' + CAST(LEAST(30.0, 0.1 * POWER(5, @RetryCount - 1)) AS VARCHAR(10)) + '00', 12)
					WAITFOR DELAY  @BackoffDelay
				 END
			  ELSE
				THROW
			 IF @RetryCount >@MaxRetryCount 
				THROW
		END CATCH 
	END
	IF @PreExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Pre Execute*/' + char(13) + @PreExecuteSQL)
		EXEC( @PreExecuteSQL )
	END	

	SELECT @RunID AS RunID
		  ,@PipelineID AS PipelineID
		  ,@SourceConnectionSettings AS	SourceConnectionSettings
		  ,@TargetConnectionSettings AS	TargetConnectionSettings
		  ,@SourceSettings AS SourceSettings
		  ,@TargetSettings AS TargetSettings
		  ,@ActivitySettings AS	ActivitySettings
		  ,@PreExecuteSQL AS PreExecuteSQL
		  ,@PostExecuteSQL AS PostExecuteSQL
		  ,@Template AS	Template
		  ,@LineageKey AS LineageKey
		  ,@TableID AS TableID
END

GO

