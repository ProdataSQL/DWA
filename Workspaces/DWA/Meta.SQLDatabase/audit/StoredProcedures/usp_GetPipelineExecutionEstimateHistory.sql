/*
Description:	Return latest successful worker execution rows for a pipeline ID with SQL-computed duration seconds.
Used By:	Sean Tracey AI agent (`execute_fabric_pipeline_tool` worker estimation path)

Example:
	exec audit.usp_GetPipelineExecutionEstimateHistory 667

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE audit.usp_GetPipelineExecutionEstimateHistory
    @pipeline_id INT
AS
BEGIN
    SET NOCOUNT ON;

    IF @pipeline_id IS NULL
    BEGIN
        THROW 50000, '@pipeline_id is required.', 1;
    END;

    SELECT TOP (5)
        pl.PipelineID AS [pipeline_id],
        pl.RunID AS [run_id],
        pl.ParentRunID AS [parent_run_id],
        pl.ParentGroupID AS [parent_group_id],
        pl.StartDateTime AS [start_time],
        pl.EndDateTime AS [end_time],
        pl.[Status] AS [status],
        DATEDIFF(SECOND, pl.StartDateTime, pl.EndDateTime) AS [duration_seconds]
    FROM audit.PipelineLog AS pl
    WHERE pl.PipelineID = @pipeline_id
      AND pl.StartDateTime IS NOT NULL
      AND pl.EndDateTime IS NOT NULL
      AND pl.[Status] = 'Completed'
    ORDER BY pl.StartDateTime DESC;
END;

GO

