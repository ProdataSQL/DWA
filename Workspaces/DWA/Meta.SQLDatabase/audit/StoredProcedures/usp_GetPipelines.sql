/*
Description:	Return pipeline execution history rows from audit.PipelineLog with lookback and status filtering.
Used By:	Sean Tracey AI agent (`fabric_failed_pipelines_db_tool`)

Example:
	exec audit.usp_GetPipelines 24, N'Failed'

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE audit.usp_GetPipelines
    @lookbackhours INT = 24,
    @status NVARCHAR(20) = N'Failed'
AS
BEGIN
    SET NOCOUNT ON;

    IF @lookbackhours IS NULL OR @lookbackhours < 1
        SET @lookbackhours = 24;

    DECLARE @normalized_status NVARCHAR(20) =
        LOWER(LTRIM(RTRIM(ISNULL(@status, N'Failed'))));

    IF @normalized_status NOT IN
    (
        N'all',
        N'failed',
        N'completed',
        N'started',
        N'in_progress',
        N'in progress',
        N'unhealthy'
    )
    BEGIN
        THROW 50000, 'Invalid @status value.', 1;
    END;

    SELECT
        pl.RunID,
        pl.PipelineID,
        pl.PackageGroup,
        pl.StartDateTime,
        pl.Status
    FROM audit.PipelineLog AS pl
    WHERE
        pl.StartDateTime >= DATEADD(HOUR, -@lookbackhours, GETDATE())
        AND
        (
            @normalized_status = N'all'
            OR (@normalized_status = N'failed' AND pl.Status = N'Failed')
            OR (@normalized_status = N'completed' AND pl.Status = N'Completed')
            OR (@normalized_status IN (N'started', N'in_progress', N'in progress') AND pl.Status = N'Started')
            OR (@normalized_status = N'unhealthy' AND (pl.Status <> N'Completed' OR pl.Status IS NULL))
        )
    ORDER BY pl.StartDateTime DESC;
END;

GO

