/*
Description:Materialize VIEWs into _staging table
Example:	exec dwa.[usp_TableLoad_PrestageView] NULL, 2
			exec dwa.[usp_TableLoad_PrestageView] 'dwa.usp_TableLoad_PrestageView', NULL  --Invalid Example
History:	20/02/2025 Created		
*/
CREATE  PROC [dwa].[usp_TableLoad_PrestageView] @TargetObject [sysname],@TableID [int] AS
BEGIN
	--SET XACT_ABORT ON 
	BEGIN 
	SET NOCOUNT ON
	DECLARE @sql nvarchar(max)
	      , @IsView bit =0
	      , @TableName sysname 
	      , @SourceObject sysname
		  , @PrestageTargetObject sysname /* Full Name of Final Target Table if prestaging */

	SELECT @TableName =TableName,  @TargetObject = t.SchemaName + '.' + t.TableName ,@SourceObject =t.SourceObject
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)+'_staging'
	FROM Meta.config.edwTables t 
	WHERE t.TableID = @TableID
	

    IF @TargetObject IS NULL
	BEGIN
		RAISERROR ('No Rule found in edwTables for %s',16,1,@SourceObject)		
	END

	IF (SELECT OBJECTPROPERTY(OBJECT_ID(@SourceObject), 'IsView') AS [IsView]) = 1
		SET @IsView = 1	
	ELSE
		BEGIN
		SET @IsView = 0
		RAISERROR ('No valid view found for Targetobject: %s',16,1,@Targetobject)		
		END

	PRINT '/* --Prestaging VIEW data into staging table--*/' + char(13) + char(10) +  'exec dwa.[usp_TableLoad_PrestageView] @TargetObject=''' + @TargetObject+''',@TableID=' + convert(varchar, @TableID)+  char(10) 
	IF @IsView = 1
	BEGIN							
			IF object_id(@PrestageTargetObject) IS NOT NULL
			BEGIN
				SET @sql = 'DROP TABLE ' + @PrestageTargetObject
				PRINT @sql   
				EXEC (@sql)
			END
	
			SET @sql = 'SELECT * INTO ' +@PrestageTargetObject  + ' FROM ' +@SourceObject	
			PRINT  @sql   
			EXEC (@sql)		
	END

	END
END