/*
Description:	Log Error for a Pipeline
Used by:		ADF Pipeline-Worker
Example:		
	 exec [audit].[usp_PipelineEndFailure] 'FED89103-A6A6-4DE9-95C8-72F7FFEB029F','ErrorCode','ErrorMessage'

History:
	22/08/2021 Bob, Created for Fabric DWA
	03/06/2025 Kristan, added to Pipeline-Worker

*/
CREATE   PROC [audit].[usp_PipelineEndFailure] @LineageKey uniqueidentifier,@ErrorCode [varchar](max),@ErrorMessage [varchar](max) AS
BEGIN
	SET NOCOUNT ON 
	UPDATE [audit].PipelineLog 
			SET Status='Failed', ErrorCode=coalesce(@ErrorCode,'Inner Activity'), ErrorMessage=coalesce(@ErrorMessage,'Inner Activiy Failed. Check Monitoring Hub '+ MonitoringUrl)
	WHERE LineageKey=@LineageKey and Status <> 'Failed'
		

END

GO

