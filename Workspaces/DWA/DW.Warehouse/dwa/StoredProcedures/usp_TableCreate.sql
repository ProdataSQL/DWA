/*
Description: Create a TABLE with CREATE TABLE SYNTAX. We often need to do this instead of CTAS due to IDENTITY
Example:
		[dwa].[usp_TableCreate]  1	/* Dim  */
		[dwa].[usp_TableCreate] 7 /* Fact */
History:
		13/01/2021 Bob, Created
*/
CREATE   PROC [dwa].[usp_TableCreate] @TableID [int],@TargetObject [varchar](512) =NULL AS
BEGIN	
	SET NOCOUNT ON
	BEGIN TRY
	DECLARE @ExecutionKey INT 
		, @TargetSchema sysname
		, @TargetTable sysname
		, @TableType varchar(10)
		, @ColumnStore bit
		, @SourceType varchar(10)
		, @Identity bit 
		, @sql nvarchar(4000)
		, @TargetColumns nvarchar(max)
		, @BusinessKeys nvarchar(4000)
		, @PrimaryKey sysname
		, @SourceColumns nvarchar(4000)
		, @SourceObject sysname
		, @Columns nvarchar(4000)
		, @RelatedBusinessKeys nvarchar(4000)

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject)

	SELECT @TargetSchema =t.SchemaName, @TargetTable =t.TableName, @TableType =t.TableType , @SourceObject =t.SourceObject, @PrimaryKey=t.PrimaryKey, @BusinessKeys =t.BusinessKeys
	FROM Meta.config.edwTables t WHERE (t.SchemaName + '.' +  t.TableName =@TargetObject) OR t.TableName =@TargetObject OR t.TableID = @TableID

	IF @TargetTable is not null SET @TargetObject=quotename(coalesce(@TargetSchema,'dbo')) + '.' + quotename(@TargetTable)
	IF @TargetTable is null
	BEGIN
		SET @TargetObject=coalesce(@TargetObject, convert(varchar(10),@TableID))
		RAISERROR ('No meta data found in edwTables for %s',16,1,@TargetObject)
	END

	IF  OBJECT_ID(@TargetObject) is NULL 
	BEGIN
		SELECT  @PrimaryKey = coalesce(@PrimaryKey, case when charindex (',',@BusinessKeys) =0 then @BusinessKeys end    ,replace(@TargetTable, 'Dim','') + 'Key')		
		SELECT @SourceColumns =string_agg(c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject)

		SET @SQL = 'CREATE TABLE ' + @TargetObject + '(' +  char(13) 
		exec [dwa].[usp_TableCreateColumns]  @TableID, @Columns output
		SET @sql=@sql + @Columns	+ char(13) +  ');'	
		PRINT @sql 
		exec (@sql)
		SET @sql = NULL

		/*ALTER command to add Constraint*/
		IF @TableType='Dim' 
			SET @sql = 'ALTER TABLE ' +@TargetObject+' ADD CONSTRAINT [PK_' + @TargetTable + '] PRIMARY KEY NONCLUSTERED (' + @PrimaryKey + ' ASC) NOT ENFORCED;'

		PRINT @sql 
		exec (@sql)
		/*
		IF @TableStructure like 'CLUSTERED INDEX%'
		BEGIN
			DECLARE @cxKeys sysname
			SET @cxKeys = substring(@TableStructure, charindex('(',@TableStructure)+1,  charindex(')',@TableStructure)-charindex('(',@TableStructure)-1 )
			SET @sql ='CREATE CLUSTERED INDEX CX_' + @TargetTable  + ' ON ' + @TargetObject + ' (' + @cxKeys + ');'
			PRINT @sql
			exec (@sql)
		END*/		
	END
	/* NOT SUPPORTED
	/* Create PK and Indexes if Missing*/
	IF @TableType='Dim' 
	BEGIN
		IF NOT EXISTS (SELECT * From sys.indexes i where object_id=object_id(@TargetObject)  and i.name ='UX_' + @TargetTable)
		BEGIN
			IF charindex(@BusinessKeys,',') > 0 
			BEGIN
				SET @sql ='CREATE INDEX [UX_' + @TargetTable + '] ON ' + @TargetObject + '('
				SET @sql=@sql + @PrimaryKey
				SET @sql=@sql + ' )'
				PRINT @sql
				exec (@sql)
			END
		END
		
	END
	*/

	END TRY 
	BEGIN CATCH
		THROW
	END CATCH 
END