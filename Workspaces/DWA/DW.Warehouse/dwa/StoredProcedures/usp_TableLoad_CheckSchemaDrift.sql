/*
Description: Compare Source and Target Tables to see if Schema Drift. Return 1 or 0
Used By:	 usp_TableLoad 
History:	 20/02/2025 Created		
			 15/03/2025 Bob, Tuning to reduce queries
*/
CREATE   PROC [dwa].[usp_TableLoad_CheckSchemaDrift] @SourceObjectID int, @TargetObjectID int, @PrimaryKey sysname, @BusinessKeys varchar(4000) , @RelatedTables varchar(4000) , @RowChecksum bit,@NonKeyColumns varchar(4000),  @IsSchemaDrift bit OUT AS
BEGIN
	SET NOCOUNT ON
	DECLARE @CheckSumSource int
		  , @CheckSumTarget int

	/*Check Tables with surrogate FKs*/
	IF len(coalesce(@RelatedTables,'')) >0
		SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name  , sc.tname ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
		FROM (
			SELECT c.name, st.name AS tname, c.max_length, c.precision, c.scale, c.is_nullable
			FROM sys.schemas s
			INNER JOIN sys.tables t ON s.schema_id = t.schema_id
			INNER JOIN sys.columns c ON t.object_id = c.object_id
			INNER JOIN sys.types st ON st.user_type_id = c.user_type_id
			WHERE s.name + '.' + t.name IN ( SELECT ltrim(value) FROM string_split(@RelatedTables,',')) AND c.column_id =1
			UNION ALL
			SELECT  c.name , st.name as tname ,c.max_length ,c.precision,c.scale,c.is_nullable
			FROM sys.columns c			 
			INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
			WHERE c.object_id = @SourceObjectID  AND c.name in (select ltrim(value) from string_split(@NonKeyColumns ,','))
			UNION ALL 
			SELECT 'RowChecksum','int',4,10,0,0
			WHERE @RowChecksum=1

		)sc
	ELSE
		SELECT @CheckSumSource = checksum(string_agg(checksum(sc.name  , sc.tname ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',')  WITHIN GROUP (Order by sc.name))
		FROM (
			SELECT  c.name , st.name as tname ,c.max_length ,c.precision,c.scale,c.is_nullable
			FROM sys.columns c			 
			INNER JOIN sys.types st ON c.user_type_id = st.user_type_id
			WHERE c.object_id = @SourceObjectID  AND c.name not in ( 'FileName','LineageKey','RowChecksum','RowVersionNo',coalesce(@PrimaryKey,''))
			UNION ALL 
			SELECT 'RowChecksum','int',4,10,0,0
			WHERE @RowChecksum=1
			) sc

	SELECT @CheckSumTarget = checksum(string_agg(checksum(sc.name , st.name ,sc.max_length ,sc.precision,sc.scale,sc.is_nullable),',') WITHIN GROUP (Order by sc.name))
	FROM sys.columns sc			 
	INNER JOIN sys.types st ON sc.user_type_id = st.user_type_id
	WHERE sc.object_id = @TargetObjectID  AND sc.name not in ('FileName','LineageKey','RowVersionNo',coalesce(@PrimaryKey,''))

	SET @IsSchemaDrift = CASE WHEN @CheckSumSource = @CheckSumTarget then 0 else 1 end
	
END