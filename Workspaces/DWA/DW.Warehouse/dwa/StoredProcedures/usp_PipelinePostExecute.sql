/*
Description:	Log Pipline Post Execute Worker
Used By:		ADF Pipeline-Worker 
  EXEC [dwa].[usp_PipelinePostExecute] 'AB839FF1-D84D-4D78-AA11-56108971B03E' ,2
History:	
	19/09/2023 Deepak, Created
	04/06/2025 Kristan, Removed code stuffing
*/
CREATE PROC [dwa].[usp_PipelinePostExecute] @RunID [char](36), @PipelineID [int] AS
BEGIN
	SET NOCOUNT ON;	
	DECLARE @PostExecuteSQL   VARCHAR(4000)
		   
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