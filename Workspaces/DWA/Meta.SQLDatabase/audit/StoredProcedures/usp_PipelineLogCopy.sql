



/*
Description:	Extended Logging for Copy Activity
Used by:		Extract-SQL and any Copy Activity
Example:		
	exec [audit].[usp_PipelineLogCopy] 'FED89103-A6A6-4DE9-95C8-72F7FFEB029F','{
	"dataRead": 594,
	"dataWritten": 1509,
	"filesWritten": 1,
	"sourcePeakConnections": 1,
	"sinkPeakConnections": 1,
	"rowsRead": 22,
	"rowsCopied": 22,
	"copyDuration": 15,
	"throughput": 0.074,
	"errors": [],
	"usedParallelCopies": 1,
	"executionDetails": [
		{
			"source": {
				"type": "SqlServer"
			},
			"sink": {
				"type": "Lakehouse"
			},
			"status": "Succeeded",
			"start": "5/2/2024, 10:18:28 AM",
			"duration": 15,
			"usedParallelCopies": 1,
			"profile": {
				"queue": {
					"status": "Completed",
					"duration": 0
				},
				"transfer": {
					"status": "Completed",
					"duration": 8,
					"details": {
						"readingFromSource": {
							"type": "SqlServer",
							"workingDuration": 0,
							"timeToFirstByte": 0
						},
						"writingToSink": {
							"type": "Lakehouse",
							"workingDuration": 0
						}
					}
				}
			},
			"detailedDurations": {
				"queuingDuration": 0,
				"timeToFirstByte": 0,
				"transferDuration": 8
			}
		}
	],
	"dataConsistencyVerification": {
		"VerificationResult": "NotVerified"
	}
}'

History:
	22/08/2021 Bob, Ceeated for Fabric DWA

*/
CREATE PROC [audit].[usp_PipelineLogCopy] @LineageKey uniqueidentifier,@Json varchar(8000)
AS
BEGIN
	SET NOCOUNT ON 
	DECLARE @Status varchar(100)
	DECLARE @errors varchar(8000)
	DECLARE @RowCount bigint
	DECLARE @copyDuration int
	DECLARE @DataWritten bigint
	DECLARE @DataRead bigint
	DECLARE @ErrorCode varchar(4000)
	DECLARE @ErrorMessage varchar(4000)
	DECLARE @RetryCount INT =0
	DECLARE @MaxRetryCount INT =3
	DECLARE @Success    BIT =0

	SELECT @errors=value
	FROM OPENJSON (@Json ) t
	WHERE [key]='errors'

	IF @errors ='[]' 
		SET @Status='Completed'
	ELSE
	BEGIN
		SET @Status	='Failed'
		SELECT @ErrorCode = Code, @ErrorMessage =[Message]
		FROM OPENJSON (@errors) 
		WITH (
			Code varchar(4000) ,
			Message varchar(4000) 
		);
	END

	select @RowCount = rowsRead, @CopyDuration=copyDuration, @DataRead =dataRead, @DataWritten=dataWritten
	FROM OPENJSON (@Json) 
	WITH (
		dataRead int ,
		dataWritten int ,
		rowsRead bigint ,
		copyDuration int 
	);


	WHILE @Success = 0
	BEGIN
		BEGIN TRY 
			UPDATE audit.PipelineLog 
				SET [Status]=@Status, ErrorCode=@ErrorCode, ErrorMessage=@ErrorMessage, CopyDuration=@copyDuration, RowsRead=@RowCount, DataRead=@DataRead, DataWritten=@DataWritten, ExtendedLog=@Json
			WHERE LineageKey=@LineageKey
			SET @Success = 1
		END TRY
		BEGIN CATCH
			 IF ERROR_NUMBER() IN (24556) /* Update Concurrency Error */
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					WAITFOR DELAY '00:00:00.100'
					--INSERT INTO Audit.dbo.Dump
					--VALUES (@LineageKey, ERROR_NUMBER())
				 END
			  ELSE
				THROW
			 IF @RetryCount >@MaxRetryCount 
				THROW
		END CATCH 
	END
END

GO

