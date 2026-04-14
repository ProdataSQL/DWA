/*
Description:	Return current pipeline health rows from devops.PipelineStatus with semantic status filtering.
Used By:	Sean Tracey AI agent (`fabric_pipeline_health_db_tool`)

Example:
	exec devops.usp_GetPipelineHealth N'all'
	exec devops.usp_GetPipelineHealth N'failed'

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE devops.usp_GetPipelineHealth
    @status NVARCHAR(20) = 'all'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @normalized_status NVARCHAR(20) =
        LOWER(LTRIM(RTRIM(ISNULL(@status, 'all'))));

    SELECT
        PackageGroup,
        ChildPackageGroup,
        PipelineID,
        Status,
        MonitoringUrl,
        StartDateTime,
        EndDateTime,
        DurationSecs,
        ExtendedLog,
        ErrorMessage,
        Pipeline,
        Template,
        RunID,
        Stage
    FROM devops.PipelineStatus
    WHERE
        @normalized_status = 'all'
        OR (@normalized_status = 'healthy' AND Status = 'Completed')
        OR (@normalized_status = 'unhealthy' AND Status <> 'Completed')
        OR (@normalized_status = 'failed' AND Status = 'Failed')
        OR (@normalized_status IN ('in_progress', 'in progress') AND Status = 'Started')
    ORDER BY devops.PipelineStatus.StartDateTime DESC;
END;

GO

