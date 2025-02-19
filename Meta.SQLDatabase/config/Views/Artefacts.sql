
CREATE View [config].[Artefacts] AS 
SELECT display_name as artefact, id as artefact_id, workspace_id , description
FROM [dbo].[dict_artefacts]
WHERE type in ('Notebook', 'DataPipeline')

GO

