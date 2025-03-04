


/*
Description:	Log Error for a Pipeline
Used by:		ADF Pipeline-Worker
Example:		
	 exec [audit].[usp_PipelineEndFailure] 'FED89103-A6A6-4DE9-95C8-72F7FFEB029F','ErrorCode','ErrorMessage'

History:
	22/08/2021 Bob, Ceeated for Fabric DWA
	04/06/2024 Aidan, Added Retry to deal with transient retry issue
	31/10/2024 Kristan, Changed to dwa schema
*/
CREATE  PROC [audit].[usp_PipelineEndFailure] @LineageKey uniqueidentifier,@ErrorCode [varchar](max),@ErrorMessage [varchar](max) AS
BEGIN
	SET NOCOUNT ON 
	DECLARE  @RetryCount			int = 0
	        ,@MaxRetryCount			    int = 5
			,@Success					BIT = 0
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 	
			UPDATE audit.PipelineLog 
					SET Status='Failed', ErrorCode=coalesce(@ErrorCode,'Inner Activity'), ErrorMessage=coalesce(@ErrorMessage,'Inner Activiy Failed. Check Monitoring Hub '+ MonitoringUrl)
			WHERE LineageKey=@LineageKey and Status <> 'Failed'
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

END

GO

