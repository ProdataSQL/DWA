/*
Description:	CTAS Operation for Table Load framework - Execute main usp_TableLoad proc
Example:    DECLARE @IsSchemaDrift BIT
			EXEC dwa.[usp_TableLoad_CheckSchemaDrift] 4, @IsSchemaDrift OUTPUT
			PRINT @IsSchemaDrift

			DECLARE @IsSchemaDrift BIT
			EXEC dwa.[usp_TableLoad_CheckSchemaDrift] 7, @IsSchemaDrift OUTPUT
			PRINT @IsSchemaDrift
History:	25/08/2022 Deepak, Created		
*/
CREATE   PROC [dwa].[usp_TableLoad_CheckSchemaDrift] @TableID [int] , @IsSchemaDrift bit OUT AS
BEGIN
	--SET XACT_ABORT ON 	
	BEGIN 
	SET NOCOUNT ON
	DECLARE @CheckSumSource int
		  , @CheckSumTarget int
		  , @SourceObject varchar(128)
		  , @TargetObject varchar(512)
		  , @PrimaryKey   varchar(512)
		  , @BusinessKeys varchar(512)
		  , @Dimensions   varchar(512)

	SELECT  @SourceObject =t.SourceObject 
	, @TargetObject = t.SchemaName + '.' +  t.TableName
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID	

	SELECT @BusinessKeys = string_agg(r.BusinessKeys, ','),@PrimaryKey = string_agg(r.PrimaryKey, ','), @Dimensions = string_agg(r.SchemaName + '.' + r.TableName, ',')
	FROM Meta.config.edwTableJoins j
	INNER JOIN Meta.config.edwTables r ON j.RelatedTableID = r.TableID
	WHERE j.TableID = @TableID
	
	/*Check Tables with surrogate FKs*/
	IF @BusinessKeys IS NOT NULL 
		BEGIN
		SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name , sc.tname ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
		FROM (
			SELECT c.name, st.name AS tname, c.max_length, c.precision, c.scale, c.is_nullable
			FROM sys.schemas s
			INNER JOIN sys.tables t ON s.schema_id = t.schema_id
			INNER JOIN sys.columns c ON t.object_id = c.object_id
			INNER JOIN sys.types st ON st.user_type_id = c.user_type_id
			WHERE s.name + '.' + t.name IN ( SELECT ltrim(value) FROM string_split(@Dimensions,',')) AND c.name IN (SELECT ltrim(value) FROM string_split(@PrimaryKey,','))
			UNION ALL
			SELECT  c.name , st.name as tname ,c.max_length ,c.precision,c.scale,c.is_nullable
			FROM sys.columns c			 
			INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
			WHERE c.object_id = object_id(@SourceObject)  AND c.name NOT IN ('RowVersionNo','RowChecksum')
			AND c.name NOT IN (SELECT ltrim(value) FROM string_split(@BusinessKeys,','))
		)sc
		END
	ELSE
		BEGIN
			SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name , st.name ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
			FROM sys.columns sc			 
			INNER JOIN sys.types st ON sc.user_type_id = st.user_type_id
			WHERE sc.object_id = object_id(@SourceObject) AND sc.name NOT IN ('RowVersionNo','RowChecksum');
		END

	SELECT @CheckSumTarget = checksum(string_agg(checksum(sc.name , st.name ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',') WITHIN GROUP (Order by sc.name))
	FROM sys.columns sc			 
	INNER JOIN sys.types st ON sc.user_type_id = st.user_type_id
	WHERE sc.object_id = object_id(@TargetObject)  AND sc.name NOT IN ('RowVersionNo','RowChecksum');

	IF @CheckSumSource <> @CheckSumTarget
			SET @IsSchemaDrift = 1
	END
END