/* Description: Return the Column Meta Data DDL for a CREATE TABLE, CTAS, Polybase or other method in SQLDB or SQLDW 
Example:
		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns]  2, @Columns output	/* Dim  */
		PRINT @Columns

		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns] 7,@Columns output  /* Fact */
		PRINT @Columns

		DECLARE  @Columns nvarchar(max)
		exec [dwa].[usp_TableCreateColumns]  4,@Columns output  /* Dim */
		PRINT @Columns
History:
		02/08/2023 Deepak, Created
*/
CREATE   PROC [dwa].[usp_TableCreateColumns] @TableID [int],@Columns [varchar](4000) OUT AS
BEGIN
	
	--SET XACT_ABORT ON
	SET NOCOUNT ON;

	BEGIN TRY
	DECLARE	@SourceObject sysname
		, @RelatedBK nvarchar(4000)
		, @BusinessKeys nvarchar(4000)
		, @PrimaryKey  sysname
		, @TableType sysname
		, @ColumnsPK nvarchar(4000)
		, @DeDupeFlag bit             /* Flag to check if DeDupeRows is ON for TableID */
        , @HideColumns nvarchar(4000) /* Array of Columns to Hide */
		, @PKs VARCHAR(400)
		, @BaseDims VARCHAR(400)

	SELECT @SourceObject =SourceObject, @BusinessKeys=BusinessKeys, @TableType=TableType, @PrimaryKey=PrimaryKey,@DeDupeFlag = DedupeFlag
	FROM Meta.config.edwTables 
	WHERE TableID=@TableID
	
	SELECT @RelatedBK = string_agg(r.BusinessKeys, ',')  
	FROM Meta.config.edwTableJoins j  
	INNER JOIN Meta.config.edwTables r on j.RelatedTableID =r.TableID
	WHERE j.TableID=@TableID

	IF @DeDupeFlag=1
       SET @HideColumns =coalesce(@HideColumns + ',','')  + 'RowVersionNo';

	/* Primary Keys (if any) from joined table if Fact or Star Schema) */
	SELECT @PKs = string_agg(r.PrimaryKey, ','), @BaseDims = string_agg(r.SchemaName + '.' + r.TableName, ',')
	FROM Meta.config.edwTableJoins j
	INNER JOIN Meta.config.edwTables r ON j.RelatedTableID = r.TableID
	WHERE j.TableID = @TableID

	SET @ColumnsPK = (SELECT string_agg( c.name + ' ' + d.name  + case when c.collation_name is not null then '(' + convert (varchar(50),COALESCE(c.max_length/(length/prec),c.max_length) ) + ')'  else '' end    			+ CASE WHEN c.is_nullable=0 then ' NOT NULL' ELSE ' NULL' END 
			, char(13)  + char(9) + ','  ) WITHIN GROUP ( ORDER BY column_id) 
	FROM sys.schemas s
	INNER JOIN sys.tables t ON s.schema_id = t.schema_id
	INNER JOIN sys.columns c ON t.object_id = c.object_id
	INNER JOIN sys.types d ON d.user_type_id = c.user_type_id
	LEFT  JOIN sys.systypes stt ON stt.name = t.name AND stt.name IN ('varchar', 'nvarchar', 'nchar')
	WHERE s.name <> 'sys'
		AND s.name + '.' + t.name IN ( SELECT ltrim(value) FROM string_split(@BaseDims,','))
		AND c.name IN (SELECT ltrim(value) FROM string_split(@PKs,','))
	)
	
	IF @ColumnsPK is NOT NULL 
		SET @Columns = coalesce(@Columns + char(13) + char(9)+ ','  , char(9) )  + @ColumnsPK
	
	SET @columns = coalesce(@columns + CHAR(13) + CHAR(9) + ',', CHAR(9)) + (
		SELECT TOP 255 string_agg(c.name + ' ' + t.name 
            + CASE WHEN c.collation_name IS NOT NULL THEN '(' + CONVERT(VARCHAR(50), coalesce(c.max_length / (length / prec), c.max_length)) + ')' 
                   WHEN t.name = 'decimal' THEN '(' + CONVERT(VARCHAR(10), c.precision) + ',' + CONVERT(VARCHAR(10), c.scale) + ')' 
                   ELSE '' 
               END 
            + CASE WHEN bk.columnname IS NOT NULL OR c.is_nullable = 0 THEN ' NOT NULL' 
                   ELSE ' NULL' 
              END, CHAR(13) + CHAR(9) + ',') WITHIN	GROUP (ORDER BY column_id)
		FROM sys.columns c
		LEFT JOIN (SELECT ltrim(value) AS columnname FROM string_split(@businesskeys, ',')) bk ON c.name = bk.columnname
		INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
		LEFT JOIN sys.systypes stt ON stt.name = t.name AND stt.name IN ('varchar', 'nvarchar', 'nchar')
		WHERE c.object_id = object_id(@sourceobject) 
        AND c.name NOT IN (SELECT ltrim(value)	FROM string_split(@relatedbk  , ',')) 
        AND c.name NOT IN (SELECT ltrim(value)	FROM string_split(@hidecolumns, ','))
		)
	END TRY 
	BEGIN CATCH
		THROW
	END CATCH 
END