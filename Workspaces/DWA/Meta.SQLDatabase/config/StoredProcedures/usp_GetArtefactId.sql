/*
Description:	Resolve artefact IDs from dbo.dict_artefacts for worker/controller pipeline item lookup.
Used By:	Sean Tracey AI agent (`fabric_list_items_db_tool`, `execute_fabric_pipeline_tool`)

Example:
	exec config.usp_GetArtefactId 'Pipeline-Worker'
	exec config.usp_GetArtefactId 'Pipeline-Controller'
	exec config.usp_GetArtefactId 'all'
	exec config.usp_GetArtefactId

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE config.usp_GetArtefactId
    @artefact_name VARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        display_name,
        id
    FROM dbo.dict_artefacts
    WHERE
    (@artefact_name IS NULL AND display_name IN ('Pipeline-Worker', 'Pipeline-Controller'))
        OR 
        LOWER(@artefact_name) = 'all'
        OR LOWER(display_name) = LOWER(@artefact_name)  
    ORDER BY
        CASE
            WHEN LOWER(display_name) = 'pipeline-worker' THEN 1
            WHEN LOWER(display_name) = 'pipeline-controller' THEN 2
            ELSE 3
        END;
END;

GO

