CREATE TABLE [config].[aiPrompts] (
    [PromptID]            INT            NOT NULL,
    [prompt_name]         VARCHAR (8000) NULL,
    [description]         VARCHAR (8000) NULL,
    [instructions]        VARCHAR (MAX)  NULL,
    [input]               VARCHAR (MAX)  NULL,
    [model]               VARCHAR (8000) NULL,
    [json_schema]         VARCHAR (MAX)  NULL,
    [ai_endpoint]         VARCHAR (8000) NULL,
    [api_family]          VARCHAR (8000) CONSTRAINT [DF_aiPrompts_api_family] DEFAULT ('responses') NOT NULL,
    [api_provider]        VARCHAR (8000) CONSTRAINT [DF_aiPrompts_api_provider] DEFAULT ('azure_openai') NOT NULL,
    [temperature]         NUMERIC (3, 2) CONSTRAINT [DF_aiPrompts_temperature] DEFAULT ((0.3)) NULL,
    [SourceConnnectionID] INT            NULL,
    [ExtraSettings]       VARCHAR (MAX)  NULL,
    [ActivitySettings]    VARCHAR (MAX)  NULL,
    CONSTRAINT [PK_aiPrompts] PRIMARY KEY CLUSTERED ([PromptID] ASC)
);


GO

/*
	Update Activiy Settings for AI Prompts

*/
CREATE   TRIGGER [config].[TR_aiPrompt_Update] 
   ON  [config].[aiPrompts]
   AFTER UPDATE, INSERT
AS 
BEGIN
	SET NOCOUNT ON;

	UPDATE p
	SET p.ActivitySettings = dbo.ufn_BuildChatPayloadJson (
		i.model, i.api_family, i.instructions, i.input, 
		i.json_schema, i.temperature, i.ExtraSettings
	)
	FROM config.aiPrompts AS p
	JOIN inserted AS i
		ON i.PromptID = p.PromptID;

	UPDATE p 
	set p.ActivitySettings= ai.ActivitySettings
	FROM config.Pipelines p 
	INNER JOIN inserted i on i.PromptID = p.PromptID
	INNER JOIN config.aiPrompts ai on ai.PromptID=i.PromptiD


END

GO

