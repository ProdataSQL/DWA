

/*
Description:	Complete a Pipeline
Used By:		ADF Pipeline-Worker 

-- Example
	exec [audit].[usp_PipelineStart] 37, '7EEFA88E-D9F9-4204-AC47-97F2FC3362CE', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82', 2
	SELECT * FROM audit.PipelineLog ORDER BY StartDateTime DESC 
	exec audit.usp_PipelineEndSuccess 1,'7EEFA88E-D9F9-4204-AC47-97F2FC3362CE'
History:	
	30/04/2024 Bob, Migrated to Fabric
	04/06/2024 Aidan, Added Backoff Algorithm
	31/10/2024 Kristan, Changed schema to dwa
*/
CREATE PROC [audit].[usp_PipelineEndSuccess]  
	@LineageKey uniqueidentifier
AS
BEGIN
	SET NOCOUNT ON;	
	DECLARE @PostExecuteSQL varchar(8000)
	DECLARE @DateTime datetime
	DECLARE @RetryCount INT =0
	DECLARE @MaxRetryCount INT =3
	DECLARE @Success    BIT =0

	SELECT @PostExecuteSQL = coalesce(p.PostExecuteSQL , pg.PostExecuteSQL)
	,@DateTime = GETDATE()
	FROM [config].[Pipelines] p 
	INNER JOIN [config].[PipelineGroups] pg on pg.PipelineGroupID =p.PipelineGroupID
			
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 
			UPDATE [audit].PipelineLog	
			SET 
				Status='Completed'
				, EndDateTime =@DateTime
			WHERE LineageKey = @LineageKey
			AND EndDateTime is null
			SET @Success = 1 
		END TRY
		BEGIN CATCH
			IF ERROR_NUMBER() IN (24556)
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					DECLARE @BackoffDelay varchar(12) = RIGHT('00:00:' + CAST(LEAST(30.0, 0.1 * POWER(5, @RetryCount - 1)) AS VARCHAR(10)) + '00', 12)
					WAITFOR DELAY  @BackoffDelay
				END
			ELSE
				THROW
			IF @RetryCount > @MaxRetryCount
				THROW
		END CATCH 
	END

	
	IF @PostExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Post Execute*/' + char(13) + @PostExecuteSQL)
		EXEC( @PostExecuteSQL )
	END	

END

GO

