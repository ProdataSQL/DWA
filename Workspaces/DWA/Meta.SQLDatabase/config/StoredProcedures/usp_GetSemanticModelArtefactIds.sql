/*
Description:	Resolve semantic model artefact IDs from dbo.dict_artefacts for one or more dataset display names.
Used By:	Sean Tracey AI agent (`fabric_semantic_model_ids_db_tool`)

Example:
	exec config.usp_GetSemanticModelArtefactIds 'Finance-GL'
	exec config.usp_GetSemanticModelArtefactIds 'Finance-GL,Finance-AP,Finance-AR'

History:
	07/04/2026 Jim Meaney, Added header metadata (Description, Used By, Example, History).
*/

CREATE   PROCEDURE config.usp_GetSemanticModelArtefactIds
    @dataset_names NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    IF @dataset_names IS NULL OR LTRIM(RTRIM(@dataset_names)) = ''
    BEGIN
        THROW 50000, '@dataset_names is required.', 1;
    END;

    ;WITH dataset_filter AS
    (
        SELECT DISTINCT LTRIM(RTRIM([value])) AS dataset_name
        FROM STRING_SPLIT(@dataset_names, ',')
        WHERE LTRIM(RTRIM([value])) <> ''
    )
    SELECT *
    FROM dbo.dict_artefacts
    WHERE display_name IN (SELECT dataset_name FROM dataset_filter)
      AND [type] = 'SemanticModel'
    ORDER BY display_name;
END;

GO

