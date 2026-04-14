/*
Description:	Return controller execution estimate samples by package group and optional stage, grouped by ParentRunID.
Used By:	Sean Tracey AI agent (`execute_fabric_pipeline_tool` controller estimation path)

Example:
	exec audit.usp_GetPipelineControllerExecutionEstimateHistory 'AW', 'ALL'
	exec audit.usp_GetPipelineControllerExecutionEstimateHistory 'AW', 'Extract'

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE audit.usp_GetPipelineControllerExecutionEstimateHistory
    @package_group NVARCHAR(255),
    @stage NVARCHAR(50) = N'ALL'
AS
BEGIN
    SET NOCOUNT ON;

    IF @package_group IS NULL OR LTRIM(RTRIM(@package_group)) = N''
    BEGIN
        THROW 50000, '@package_group is required.', 1;
    END;

    DECLARE @normalized_stage NVARCHAR(50) =
        UPPER(LTRIM(RTRIM(ISNULL(@stage, N'ALL'))));

    ;WITH scoped_rows AS
    (
        SELECT
            pl.ParentRunID,
            pl.PackageGroup,
            pl.[Status],
            pl.StartDateTime,
            pl.EndDateTime
        FROM audit.PipelineLog AS pl
        WHERE pl.PackageGroup = @package_group
          AND pl.ParentRunID IS NOT NULL
          AND
          (
              @normalized_stage = N'ALL'
              OR UPPER(LTRIM(RTRIM(ISNULL(pl.Stage, N'')))) = @normalized_stage
          )
    ),
    eligible_parent_runs AS
    (
        SELECT
            sr.ParentRunID,
            MIN(sr.StartDateTime) AS run_start_time,
            MAX(sr.EndDateTime) AS run_end_time,
            COUNT_BIG(*) AS run_row_count,
            SUM(CASE WHEN sr.[Status] <> 'Completed' OR sr.[Status] IS NULL THEN 1 ELSE 0 END) AS non_completed_count,
            SUM(CASE WHEN sr.StartDateTime IS NULL OR sr.EndDateTime IS NULL THEN 1 ELSE 0 END) AS invalid_time_count
        FROM scoped_rows AS sr
        GROUP BY sr.ParentRunID
        HAVING
            SUM(CASE WHEN sr.[Status] <> 'Completed' OR sr.[Status] IS NULL THEN 1 ELSE 0 END) = 0
            AND SUM(CASE WHEN sr.StartDateTime IS NULL OR sr.EndDateTime IS NULL THEN 1 ELSE 0 END) = 0
    ),
    top_parent_runs AS
    (
        SELECT TOP (5)
            epr.ParentRunID,
            epr.run_start_time,
            epr.run_end_time,
            epr.run_row_count
        FROM eligible_parent_runs AS epr
        ORDER BY epr.run_start_time DESC
    )
    SELECT
        @package_group AS [package_group],
        CASE
            WHEN @normalized_stage = N'ALL' THEN N'ALL'
            ELSE @normalized_stage
        END AS [stage_filter],
        tpr.ParentRunID AS [parent_run_id],
        tpr.run_start_time AS [start_time],
        tpr.run_end_time AS [end_time],
        DATEDIFF(SECOND, tpr.run_start_time, tpr.run_end_time) AS [duration_seconds],
        tpr.run_row_count AS [run_row_count]
    FROM top_parent_runs AS tpr
    ORDER BY tpr.run_start_time DESC;
END;

GO

