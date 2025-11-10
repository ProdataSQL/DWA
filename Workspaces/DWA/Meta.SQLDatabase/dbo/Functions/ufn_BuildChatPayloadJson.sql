/*
    Retutn the JSon Payload for Both the v1 completions API or the V2 Responses API for AI LLM Calls as per:
https://platform.openai.com/docs/api-reference/completions
https://platform.openai.com/docs/api-reference/responses


History: 06/11/2025, Bob, Completed

*/
CREATE   FUNCTION [dbo].[ufn_BuildChatPayloadJson](
    @model varchar(4000)=null
	,@api_family varchar (4000) = 'chat' /* chat | responses */
    ,@instructions varchar(max)=null
    ,@input varchar(max)=null
    ,@json_schema varchar(max)=null
    ,@temperature float=null
    ,@ExtraSettings varchar(max)=null
)
RETURNS varchar(max)
WITH SCHEMABINDING
AS
BEGIN
	DECLARE @Json varchar(max)

;WITH keys AS (
	SELECT 'model' AS [key], @model AS [value], '1' AS [type] WHERE @model  IS NOT NULL
	UNION ALL
	SELECT 'messages' AS [key],
		'[ ' 
		+ COALESCE('{"role": "system", "content": "' + @instructions + '"},', '') 
		+ '{"role": "user", "content": "' + @input + '"}' 
		+ ']' AS [value],
		'4' AS [type]
	WHERE @input IS NOT NULL AND @api_family='chat'
	UNION ALL 
	SELECT 'response_format' AS [key],
		'{"type": "json_schema","json_schema":' + @json_schema + '}' AS [value],
		'4' AS [type]
	WHERE @json_schema IS NOT NULL and @api_family='chat'
	UNION ALL 
	SELECT 'text' AS [key],	'{"format": {"type": "json_schema","name": "text_schema","schema": ' +  @json_schema+ ',"strict": true}}'
	, '4' AS [type]
	WHERE @json_schema IS NOT NULL AND @api_family<>'chat'
	UNION ALL
	SELECT	'instructions' AS [key],  @instructions,  '1' as [type]
	WHERE @instructions IS NOT NULL and @api_family<>'chat'
	UNION ALL 
	SELECT 'input' AS [key],  @input,  '1' as [type]
	WHERE @input IS NOT NULL  and @api_family<>'chat'
	UNION ALL
	SELECT 'temperature' AS [key], CONVERT(varchar, @temperature) AS [value], '2' AS [type]
	WHERE @temperature IS NOT NULL 

)
SELECT @Json=  (
	SELECT '{'
		+ STRING_AGG(
			'"' + s.[key] + '":' 
			+ CASE 
				WHEN s.[type] = '1' THEN '"' + s.[value] + '"' COLLATE SQL_Latin1_General_CP1_CI_AS
				WHEN s.[type] = '0' THEN 'null'
				ELSE s.[value] COLLATE SQL_Latin1_General_CP1_CI_AS
			  END
			, ','
		)
		+ '}' as b
	FROM (
		SELECT k.[key], k.[value], k.[type]
		FROM keys k

		UNION ALL

		SELECT j.[key], j.[value], j.[type]
		FROM OPENJSON(@ExtraSettings) AS j
		LEFT JOIN keys k ON k.[key] = j.[key]
		WHERE k.[key] IS NULL
	) s
) 
RETURN @Json
END

GO

