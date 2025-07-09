
/*
Description:    Complete a Pipeline
Used By:        ADF Pipeline-Worker

-- Example
    exec [audit].[usp_PipelineStart] 37, '7EEFA88E-D9F9-4204-AC47-97F2FC3362CE', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82', 2
    SELECT * FROM audit.PipelineLog ORDER BY StartDateTime DESC 
    SELECT * FROM audit.LineageLog ORDER BY LineageKey DESC 
    exec [audit].[usp_PipelineStart] 1,'7EEFA88E-D9F9-4204-AC47-97F2FC3362CE'

History:    
    30/04/2024 Bob, Migrated to Fabric
    04/06/2024 Aidan, Added Backoff Algorithm
    04/01/2025 Aidan, Migrated to META
	03/06/2025 Kristan, Added to Pipeline-Worker
*/

CREATE PROC [audit].[usp_PipelineEndSuccess]  
    @LineageKey uniqueidentifier
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @PostExecuteSQL varchar(8000)
    DECLARE @DateTime datetime

    SELECT 
        @PostExecuteSQL = COALESCE(p.PostExecuteSQL, pg.PostExecuteSQL),
        @DateTime = GETDATE()
    FROM [config].[Pipelines] p 
    INNER JOIN [config].[PipelineGroups] pg ON pg.PipelineGroupID = p.PipelineGroupID

    UPDATE [audit].PipelineLog
    SET 
        Status = 'Completed',
        EndDateTime = @DateTime
    WHERE LineageKey = @LineageKey
      AND EndDateTime IS NULL

    IF @PostExecuteSQL IS NOT NULL 
    BEGIN
        PRINT ('/*Executing Post Execute*/' + CHAR(13) + @PostExecuteSQL)
        EXEC(@PostExecuteSQL)
    END
END

GO

