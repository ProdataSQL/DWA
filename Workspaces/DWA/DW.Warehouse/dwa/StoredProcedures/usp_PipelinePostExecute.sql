/*
Description:	Log Pipline Post Execute Worker
Used By:		ADF Pipeline-Worker 
  EXEC [dwa].[usp_PipelinePostExecute] 'AB839FF1-D84D-4D78-AA11-56108971B03E' ,2
History:	
	20/02/2025  Created
*/
CREATE PROC [dwa].[usp_PipelinePostExecute] @RunID [char](36), @PipelineID [int] AS
BEGIN
	SET NOCOUNT ON;	
	DECLARE @PostExecuteSQL   VARCHAR(4000)
		   ,@SourceDirectory  VARCHAR(512)
		   ,@TargetTableName  VARCHAR(128)
		   ,@TargetDirectory  VARCHAR(512)
		   ,@ArchiveDirectory VARCHAR(4000)
		   ,@Filename NVARCHAR(200)
		   ,@LineageKey INT
		   ,@SQL NVARCHAR(max)
	
	--Archive
	SELECT  @LineageKey = l.LineageKey, @TargetTableName =p.TargetSchemaName + '.' + p.TargetTableName, @SourceDirectory= p.SourceDirectory, @ArchiveDirectory=p.ArchiveDirectory 
	FROM audit.PipelineLog p
	INNER JOIN audit.LineageLog l ON p.RunID = l.RunID AND p.SourceObject = l.SourceObject
	WHERE p.Stage = 'Extract' AND p.RunID = @RunID AND (p.PipelineID=@PipelineID OR @PipelineID=-1 OR @PipelineID IS NULL)
	GROUP BY p.PipelineID, p.RunID, l.LineageKey, p.TargetSchemaName, p.TargetTableName, p.SourceDirectory, p.ArchiveDirectory	

	IF @ArchiveDirectory IS NOT NULL 
	BEGIN
		BEGIN TRY
			SET @SQL = 'SELECT @Filename= FileName FROM FabricLH.' + @TargetTableName +' GROUP BY FileName'	
			EXEC sp_executesql @SQL, N'@Filename varchar(max) output', @Filename OUTPUT	
		END TRY
		BEGIN CATCH			
			SET @Filename = COALESCE(@Filename,NULL)
		END CATCH

		IF @Filename IS NOT NULL 
			INSERT INTO dwa.ArchiveQueue (PipelineID,	RunID,	LineageKey,	TargetTableName,	SourceDirectory,	ArchiveDirectory,Filename)
			VALUES ( @PipelineID, @RunID,	@LineageKey,	@TargetTableName, @SourceDirectory, @ArchiveDirectory,	@Filename);	
	END

	--PostExecute
	SELECT @PostExecuteSQL = p.PostExecuteSQL  
	FROM audit.PipelineLog p	
	WHERE p.PipelineID = @PipelineID AND p.RunID = @RunID
	GROUP BY p.PostExecuteSQL  
	
	IF @PostExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Post Execute*/' + char(13) +  @PostExecuteSQL)
		EXEC(@PostExecuteSQL )
	END	
END