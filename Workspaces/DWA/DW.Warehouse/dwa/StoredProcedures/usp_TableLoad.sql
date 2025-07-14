/*
Load Table into DWH with "smart" loading: selection of SELECT INTO v INSERT INTO ,DDL Changes, FK Maintenance
Used By: LoadWorker ADF Pipeline
Example:
	exec [dwa].[usp_TableLoad] NULL,1,NULL
	[dwa].[usp_TableLoad] 'aw.DimDate' 
	[dwa].[usp_TableLoad] 'aw.FactFinance'

History:
	03/08/2023 Deepak,	Created
	14/08/2023 Shruti,	Refactoring
	06/09/2023 Deepak,	Refatoring Logging
	11/02/2025 Kristan,	Refactor out Truncate Sproc
	13/03/2025 Kristan, Added Identity feature
	15/03/2025 Bob,		Addded RowChecksum and Tuning
	18/03/2025 Kristan,	Checksum bugfix
	25/03/2025 Kristan, Added dynamic surragation
	31/03/2025 Kristan, Dedupe TablePrefix
	16/04/2025 Kristan, Hash identity method fix
	01/05/2025 Shruti,  Added Collation checks to manage Case-sensitivity.
*/
CREATE PROC [dwa].[usp_TableLoad] @TargetObject [nvarchar](512) =NULL, @TableID [int]= NULL, @RunID uniqueidentifier = NULL AS
BEGIN
	BEGIN TRY
	SET NOCOUNT ON 
	DECLARE  @SchemaName nvarchar(128)
		, @TableName nvarchar(128)
		, @TableType nvarchar(10)
		, @SourceType nvarchar(10)
		, @SourceObject nvarchar(128)
		, @SourceSchema nvarchar(128)
		, @SourceTable varchar(128)
		, @TargetSchema varchar(128)
		, @TargetTable varchar(128)
		, @TargetObjectFinal [nvarchar](512)
		, @sql nvarchar(4000)
		, @InsertColumns varchar(max)
		, @SourceColumns varchar(max)
		, @SelectColumns varchar(max)
		, @TargetColumns varchar(max)
		, @UpdateColumns varchar(max)
		, @JoinSQL nvarchar(4000)
		, @BusinessKeys varchar(4000)
		, @RelatedBusinessKeys varchar(max)
		, @PrimaryKey varchar(128)
		, @InsertCount int
		, @UpdateCount int		
		, @UpdateFlag bit		/* 1 if an Update required */
		, @TablePrefix nvarchar(128)	/* Alias for Target Table in Dynamic TSQL */
		, @AutoDrop bit			/* 1 to force a drop of Table */
		, @AutoTruncate bit			/* 1 to force Truncate Table before Load */
		, @CTAS bit				/* 1 if CTAS statement required */
		, @Exists bit			 /* 1 if data exists in TargetObject   */
		, @DedupeFlag bit		 /* 1 if We Dedupe Rows by using RowVersionNo =1*/
		, @sqlWhere varchar(4000) /* Additional Where Clause predicates */
		, @HideColumns varchar(4000) /* Array of Columns to Hide */
		, @PrestageSourceFlag bit /* 1 if Prestage Source View to Staging table */
		, @PrestageTargetObject varchar(128) /* Full Name of Final Target Table if prestaging */
		, @PrestageTargetFlag bit			/* 1 if Target Table is a prestaged and needs DeltaAction 'N,X,C' to track New, Delete, Change */
		, @PrestageJoinSQL varchar(4000)	 /* Join Criteria for Prestaging */
		, @PrestageSchema varchar(128) 
		, @DeleteDDL varchar(4000)
		, @DeleteFlag bit 
		, @InsertFlag bit
		, @SkipSqlCondition varchar(4000)
		, @DeltaTargetObject varchar(128)
		, @DeltaJoinSQL varchar(4000)	 /* Join Criteria for Delta ot DeltaTargetObject */
		, @i int
		, @Skip	bit						/* 1 if Processing to Skip */
		, @PreLoadSQL	varchar(512)		/* TSQL or Proc for Infer */
		, @PostLoadSQL varchar(512)		/* TSQL or Proc for Validate */
		, @JoinType nvarchar(25)			/* Default INNER JOIN, but can be override to LEFT */
		, @Language nvarchar(128)				/* British if dates need to be DMY */
		, @WhereJoinSQL varchar(4000)   /* Variable for JOIN SQL between Fact and View for WHERE NOT EXISTS*/ 
		, @LocalJoinSQL varchar(4000)
		, @SchemaDrift bit=0
		, @RelatedTableCount smallint =0
		, @VarHideCols VARCHAR(4000) --Temp Variable to store hide columns value
		, @IsSchemaDrift bit
		, @IsIncremental bit
		, @SCD bit
		, @Identity bit
		, @IdentityExpression varchar(max)
		, @MaxIdentity int = 0
		, @SourcePrimaryKeyFlag bit = 0
		, @RowChecksum bit
		, @SourceColumnList varchar(4000)
		, @TargetColumnList varchar(4000)
		, @SourceObjectID int
		, @TargetObjectID int
		, @BusinessKeyCount smallint
		, @LineageColumns varchar(4000) ='LineageKey,FileName' /* Columns to Skip with RowChecksum along with BK and PK */
		, @NonKeyColumns varchar(4000) = '' /* Source Columns which are not BK, PK or Lineage. used for RowChecksum and Updates */
		, @RelatedTables varchar(4000)
		, @RowVersionNoFlag bit				/* 1 if Table already has manual RowVersionNo used for dedupe*/
		, @DedupeOrderBy varchar(4000)		/* Order Clause for Dedupe */
		, @SqlWhereOuter varchar(4000)		/* Outer Where Clause */
		, @BusinessKeysQualified varchar(4000) /* Qualified Business Keys. Eg a.AccountKey,d.DateKey */
		, @NonKeyColumnsQualified varchar(4000) 
		, @TableSwapFlag bit =0				/* 1 = Load by Using TableSwap (Implies full CTAS) */
		, @ForceCollation bit = 0
		, @ColumnsCollation varchar(512) = NULL
		, @SourceCollation sysname
		, @TargetCollation sysname
	IF @TableID IS NULL
		SELECT TOP 1 @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName = @TargetObject) OR t.TableName =@TargetObject) ORDER BY TableID

	SELECT @SchemaName =SchemaName, @TableName =TableName, @SourceObject =t.SourceObject  ,@TableType =t.TableType , @TargetSchema =t.SchemaName ,  @TargetTable =t.TableName,  @SourceType=t.SourceType,  @PrimaryKey=t.PrimaryKey, @BusinessKeys =t.BusinessKeys
	, @UpdateFlag=coalesce(t.UpdateFlag, case when t.TableType='Fact'then 0 else 1 end )
	, @TablePrefix= coalesce(t.TablePrefix , case when t.TableType='Fact' THEN 'f' ELSE lower(left(replace(t.TableName, 'Dim',''),1)) END)
	, @AutoDrop=coalesce (t.AutoDrop,0)
	, @AutoTruncate =coalesce(t.AutoTruncate,0)
	, @CTAS =t.CTAS
	, @DedupeFlag = coalesce(t.DedupeFlag,0)
	, @DedupeOrderBy  = coalesce (t.DedupeOrderBy,coalesce(t.TablePrefix , case when t.TableType='Fact' THEN 'f' ELSE lower(left(replace(t.TableName, 'Dim',''),1)) END) + '.LineageKey DESC')
	, @PrestageSourceFlag =coalesce(t.PrestageSourceFlag,0)
	, @PrestageTargetFlag=coalesce(t.PrestageTargetFlag,0)
	, @PrestageSchema = coalesce(t.PrestageSchema, PARSENAME(t.SourceObject,2) )
	, @PrestageTargetObject =coalesce(t.PrestageSchema, PARSENAME(t.SourceObject,2) ) + '.'+ t.TableName 
	, @DeleteDDL=t.DeleteDDL
	, @TargetObject=t.SchemaName + '.' +  t.TableName 
	, @DeleteFlag =coalesce(t.DeleteFlag, case when t.DeleteDDL is not null then 1 else 0 end )
	, @SkipSqlCondition = t.SkipSqlCondition
	, @InsertFlag =coalesce(t.InsertFlag,1)
	, @DeltaTargetObject= t.DeltaTargetObject
	, @Skip=0
	, @PostLoadSQL =t.PostLoadSQL
	, @PreLoadSQL =t.PreLoadSQL
	, @JoinType=coalesce(t.JoinType, 'INNER') 
	, @SqlWhere =coalesce('WHERE ' + t.WhereSQL,null)
	, @Language =t.[Language]
	, @SchemaDrift =coalesce(t.SchemaDrift,0) 
	, @IsIncremental = coalesce(t.IsIncremental,0)
	, @SCD = coalesce(t.SCD,0)
	, @Identity = t.[Identity]
	, @IdentityExpression = im.Expression
	, @RowChecksum = coalesce(t.RowChecksum, coalesce(t.UpdateFlag, case when t.TableType='Fact'then 0 else 1 end ))/* Only add RowChecksum if Updating Table */
	, @RelatedTableCount = (SELECT COUNT(*) from Meta.config.edwTableJoins where TableID=@TableID)
	, @TableSwapFlag = coalesce(t.TableSwapFlag,0)
	
	FROM Meta.config.edwTables t 
	LEFT JOIN Meta.config.IdentityMethods im on im.IdentityMethod=coalesce(t.IdentityMethod,'Hash-MD5') --Default IdentityMethod
	WHERE t.TableID = @TableID

	IF @@LANGUAGE <> @Language  and coalesce(@Language,'' ) <> ''
	BEGIN
		IF @Language ='British'
			SET LANGUAGE British
		ELSE IF @Language in ('us_English','English')
			SET LANGUAGE us_English
		ELSE
			raiserror ('Unsupported Language [%s]',16,1,@Language)

		SET @sql = 'SET LANGUAGE ' + @Language
		PRINT @sql
	END

	/* PreLoadSQL */
	IF @PreLoadSQL is not null
	BEGIN
		PRINT '--PreLoadSQL Event '
		PRINT @PreLoadSQL
		PRINT ''
		exec (@PreLoadSQL)
	END

	IF charindex('.',@SourceObject) > 0
		BEGIN
			SET @SourceSchema = left( @SourceObject, charindex('.',@SourceObject)-1 )
			SET @SourceTable= substring( @SourceObject, charindex('.',@SourceObject)+1 , 255)
		END
	ELSE
		BEGIN
			SELECT @SourceSchema ='dbo', @SourceTable =@SourceObject 
			SET @SourceObject ='dbo.' + @SourceObject 
		END

	SELECT  @SourceObjectID = OBJECT_ID(@SourceObject),  @TargetObjectID = OBJECT_ID(@TargetObject)
	, @BusinessKeyCount = (SELECT COUNT(*) FROM string_split(@BusinessKeys,','))
	IF @SourceObjectID is null
		raiserror ('Missing Source Object [%s]. Check artefact exists.',16,1,@SourceObject)
	SELECT @SourceColumnList = '' + (SELECT STRING_AGG(c.name, ',') FROM sys.columns c WHERE c.object_id = @SourceObjectID)
	IF @TargetObjectID is not null 
		IF @RelatedTableCount > 0 
			SELECT @TargetColumnList = '' + (SELECT STRING_AGG(c.name, ',') FROM sys.columns c WHERE c.object_id = @TargetObjectID)
		ELSE
			SET  @TargetColumnList=@SourceColumnList
	/* Identity/Surrogate Logic */
	IF @PrimaryKey is not null 
		SELECT @SourcePrimaryKeyFlag =  CASE WHEN EXISTS (SELECT * from string_split(@SourceColumnList,',') WHERE value=@PrimaryKey) THEN 1 else 0 END
	IF @Identity=1 AND @SourcePrimaryKeyFlag = 1  
		RAISERROR ('Configuration Error: PrimaryKey in SourceObject and Identity=1 in edwTables for %s',16,1,@TargetObject)
	IF @SourcePrimaryKeyFlag=1 	SET @Identity = 0;
	SET @Identity = coalesce(@Identity, CASE WHEN @TableType = 'Dim' AND len(@PrimaryKey) >0 AND @SourcePrimaryKeyFlag=0 THEN 1 ELSE 0 END )

	IF @AutoDrop=1
		SELECT @UpdateFlag =0, @InsertFlag=0, @DeleteFlag=0, @SchemaDrift=0, @PrestageTargetFlag =1, @SchemaDrift =0
    IF @TableName is null
	BEGIN
		SET @TargetObject=coalesce(@TargetObject, convert(varchar(10),@TableID))
		RAISERROR ('No meta data found in edwTables for %s',16,1,@TargetObject)
	END
	SET @CTAS= CASE WHEN (object_id(@TargetObject) is null or @AutoDrop=1) AND coalesce(@CTAS,1)=1 OR @TableSwapFlag=1 THEN 1 ELSE 0 END 

	IF object_id(@TargetObject) IS NOT NULL AND @IsIncremental = 1
		SELECT @AutoDrop = 0, @AutoTruncate=0,@InsertFlag=1,@CTAS=0

	IF object_id(@TargetObject) IS NULL OR @AutoDrop=1 or @AutoTruncate =1  
		SET @UpdateFlag=0
    
   	IF @RunID IS NULL	SET @RunID = NEWID()
		
	IF @SkipSqlCondition IS NOT NULL
	BEGIN
		SET @sql = 'SELECT @i=CASE WHEN (' + @SkipSqlCondition + ') THEN 1 ELSE 0 END'
		EXEC sp_executesql @sql, N'@i int output', @i OUTPUT
		IF @i = 1
			SELECT @UpdateFlag = 0, @InsertFlag = 0, @DeleteFlag = 0, @AutoDrop = 0, @AutoTruncate = 0, @Skip = 1
		IF @Skip = 1
			PRINT '/* Skip Condition True(' + @SkipSqlCondition + '). Load Skipped */'
	END

	IF object_id(@SourceObject) IS NULL AND @Skip = 0
		RAISERROR ('SourceObject %s not found. Check Meta.config.edwTables for TableID %i', 16, 1, @SourceObject, @TableID)

	IF @DedupeFlag =1 and @skip=0
		SELECT @sqlWhereOuter= 'RowVersionNo=1' 
	IF @TableSwapFlag=1 
		SELECT @AutoDrop=0, @AutoTruncate =0, @UpdateFlag=0, @InsertFlag =0
	IF @AutoDrop =1 AND object_id(@TargetObject) is not null 
		exec [dwa].usp_TableDrop @TargetObject
	IF @AutoTruncate =1 AND object_id(@TargetObject) is not null 
	BEGIN
		SET @sql = 'TRUNCATE TABLE ' + @TargetObject
		PRINT  @sql 
		EXEC (@sql)
	END

	IF object_id(@TargetObject) is null 
		SELECT @Exists=0 , @IsIncremental=0
	ELSE
		BEGIN
			IF @SourceType='View'
			BEGIN
				set @sql = 'SELECT @Exists=CASE WHEN EXISTS (SELECT * FROM ' + @TargetObject + ') THEN 1 ELSE 0 END '
				exec sp_executesql @sql, N'@Exists bit output', @Exists output
			END
			ELSE
				SET @Exists=1
		END	

	IF @SourceType ='Proc' and @Skip=0
	BEGIN
		SET @sql = 'exec ' + @SourceObject 
		PRINT  @sql   
		exec (@sql)	
	END
	ELSE IF @SourceType ='View' and @Skip=0
	BEGIN			
		/* Check if collation between Lakehouse and Data Warehouse is different */
		SELECT @TargetCollation = CONVERT(sysname, DATABASEPROPERTYEX(db_name(), 'Collation'))
		SELECT TOP 1 @SourceCollation = collation_name FROM sys.columns 
			   WHERE object_id = object_id(@SourceObject) AND collation_name IS NOT NULL AND collation_name <> @TargetCollation
		SELECT @ColumnsCollation = STRING_AGG(name, ',') FROM sys.columns 
			   WHERE object_id = object_id(@SourceObject) AND collation_name IS NOT NULL AND collation_name <> @TargetCollation
		IF COALESCE(@SourceCollation, @TargetCollation) <> @TargetCollation
			SET @ForceCollation = 1

		/* Update Logic */	
		IF @DedupeFlag=1
			SET @HideColumns =coalesce(@HideColumns + ',','')  + 'RowVersionNo';

		/* Parse Related Tables */
		IF @RelatedTableCount > 0 
		WITH s as(
			SELECT value as colname FROM string_split(@ColumnsCollation, ',')
		), r as (
			SELECT r.TableID AS RelatedTableID
				, coalesce(j.JoinSQL, string_agg(@TablePrefix + '.' + ltrim(bk.value) + CASE WHEN @ForceCollation = 1 AND s.colname IS NOT NULL THEN ' COLLATE ' + @TargetCollation  ELSE '' END + ' = ' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(bk.value), ' AND ')) fk
				, r.BusinessKeys, r.SchemaName, r.TableName, r.PrimaryKey
				, min(coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1)))) TablePrefix
				, min(coalesce(j.JoinSQL, 't.' + ltrim(r.PrimaryKey) + '=' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(r.PrimaryKey))) WhereJoinSQL
				, coalesce(j.JoinType, @JoinType) AS JoinType
			FROM Meta.config.edwTables r
			INNER JOIN Meta.config.edwTableJoins j ON j.RelatedTableID = r.TableID
			CROSS APPLY string_split(r.BusinessKeys, ',') bk
			LEFT JOIN s ON s.colname = bk.value --To check for columns that needed collation
			WHERE j.TableID = @TableID
			GROUP BY r.TableID, r.BusinessKeys  , r.SchemaName, r.TableName, r.PrimaryKey, j.JoinSQL, j.JoinType
		), t AS (
			SELECT t.TableID, coalesce(t.TablePrefix, lower(left(replace(t.TableName, 'Dim', ''), 1))) AS TablePrefix
			FROM Meta.config.edwTables t
			WHERE t.TableID = @TableID
		)	
		SELECT @InsertColumns = string_agg( coalesce(j.AliasPK, r.PrimaryKey), ',') 
		, @SelectColumns=string_agg( r.TablePrefix + '.' + r.PrimaryKey  + coalesce(' AS ' + j.AliasPK,'') , ',') --+ char(13)
		, @RelatedBusinessKeys = string_agg( r.BusinessKeys, ',') 
		, @JoinSQL = string_agg(  r.JoinType+ ' JOIN ' + quotename(r.SchemaName) + '.' + quotename (r.TableName) + ' ' + r.TablePrefix	+ ' ON ' +  r.fk  ,char(13)) WITHIN GROUP (Order by j.JoinOrder)
		, @LocalJoinSQL = string_agg(r.WhereJoinSQL +' ' ,char(13))
		, @RelatedTables= string_agg(r.SchemaName + '.' + r.TableName, ',')
		, @BusinessKeysQualified = string_agg( r.TablePrefix + '.' + r.PrimaryKey ,',')
		FROM Meta.config.edwTableJoins  j 
		INNER JOIN r on j.RelatedTableID =r.RelatedTableID
		WHERE j.TableID=@TableID
		ELSE
			SET @BusinessKeysQualified = (SELECT STRING_AGG(@TablePrefix + '.' + TRIM(value), ', ') FROM STRING_SPLIT(@BusinessKeys, ','));

		IF @BusinessKeys IS NOT NULL 
			SET @WhereJoinSQL=null
		ELSE
			SET @WhereJoinSQL=coalesce(@WhereJoinSQL + ' AND ' + @LocalJoinSQL,@LocalJoinSQL)
		
		/* Check Row Does not exist in another target */
		IF @DeltaTargetObject is not null and object_id(@DeltaTargetObject) is not null and @BusinessKeys is not null AND OBJECT_ID(@TargetObject) is null
		BEGIN
			SET @DeltaJoinSql ='LEFT JOIN ' + @DeltaTargetObject+ ' t ON '
			IF @LocalJoinSQL IS NULL
				SET @DeltaJoinSql = @DeltaJoinSql +  (SELECT string_agg( @TablePrefix + '.'+ ltrim([value]) +'=' + 't.' + ltrim([value]) ,' AND ')   FROM (SELECT value from string_split(@BusinessKeys,',') ) a )		
			ELSE 
				SET @DeltaJoinSql = @DeltaJoinSql + (SELECT string_agg(rtrim(bk.value), ' AND ')  FROM string_split(rtrim(@LocalJoinSQL), ' ') bk)
			SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ', 'WHERE ')  + '('+@TablePrefix +'.RowChecksum <> t.RowChecksum OR t.RowChecksum is null)'
		END
		ELSE IF @DeltaTargetObject is not null
			SET @sqlWhere=null

		IF @InsertColumns IS NOT NULL
		SET @HideColumns = coalesce(@HideColumns+ ',' ,'') + @InsertColumns

		IF @RelatedTableCount>0
			IF EXISTS (SELECT * FROM Meta.config.edwTableJoins WHERE TableID=@TableID and (ShowBK is null or ShowBK=0))
			BEGIN 	
				SELECT @VarHideCols = (select string_agg(r.BusinessKeys, ',') 
					from Meta.config.edwTableJoins j
					inner join Meta.config.edwTables r on r.TableID=j.RelatedTableID
					where j.TableID=@TableID
					and (ShowBK is null or ShowBK=0))
				SET @HideColumns = coalesce(@HideColumns+ ',' ,'') +@VarHideCols				
			END

		SELECT @InsertColumns = case when @InsertColumns is null then '' else @InsertColumns + ',' end + (SELECT string_agg( c.name, ',') FROM sys.columns c WHERE c.object_id = @SourceObjectID and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))   )
		,@SelectColumns = case when @SelectColumns is null then '' else @SelectColumns + ',' end + (SELECT string_agg(  @TablePrefix + '.' + c.name + CASE WHEN @ForceCollation = 1 AND c.collation_name <> @TargetCollation THEN ' COLLATE ' + @TargetCollation + ' AS ' + c.name ELSE '' END, ',') FROM sys.columns c WHERE c.object_id = @SourceObjectID and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns ,',')  ) )
		,@RowVersionNoFlag = CASE WHEN EXISTS (SELECT * FROM sys.columns sc WHERE  object_id = @SourceObjectID and name='RowVersionNo') then 1 else 0 end 
		
		/* Adding Identity */

		IF @Identity = 1 AND @PrimaryKey is null
			RAISERROR ('Unsupported Identity=1 AND PrimaryKey not defined for Table %s. Check Meta.config.edwTables.',16,1, @TargetTable)	
		IF @Identity = 1 AND @Exists=1 AND @IdentityExpression like 'ROW_NUMBER%'
		BEGIN
			SET @SQL = N'SELECT @MaxIdentity = ISNULL(MAX(' + @PrimaryKey + '), 0) FROM ' + @TargetObject +';';
			EXEC sp_executesql @SQL, N'@MaxIdentity INT OUTPUT', @MaxIdentity OUTPUT;
		END
		IF @BusinessKeyCount=1
			SET @IdentityExpression=REPLACE(@IdentityExpression, 'concat(@BKs)','@BKs')
		IF @Identity=1 AND @BusinessKeys is not null
		BEGIN
			SET @InsertColumns=@PrimaryKey +','+ @InsertColumns
			SET @SelectColumns=REPLACE(REPLACE(@IdentityExpression, '@BKs', (SELECT string_agg(@BusinessKeys,','))),'@MaxID',convert(varchar(4000),@MaxIdentity)) + ' AS '+ @PrimaryKey +','+ @SelectColumns
		END
		ELSE IF @Identity=1 AND @BusinessKeys is null
		RAISERROR ('Unsupported Identity=1 AND BusinessKeys not defined for Table %s. Check Meta.config.edwTables.',16,1,@TargetTable)

		SELECT @PrimaryKey=replace(@TargetTable, 'Dim','') + 'Key'

		IF @PrestageSourceFlag=1
			BEGIN
				DECLARE @PrestageSourceTable sysname = @SourceObject+ '_Staging'
				exec [dwa].[usp_TableLoad_PrestageView] @SourceObject, @PrestageSourceTable 
				SET @SourceObject = @PrestageSourceTable
			END			
		PRINT '/*' + char(13) + char(10) + 'exec dwa.usp_TableLoad @TargetObject=''' + @TargetObject+''',@TableID=' + convert(varchar, @TableID) + ',@RunID=''' + convert (varchar(255), @RunID) +'''' +  char(13) + char(10) + '*/' ;
		
		DECLARE @UpdateColumnList varchar(4000) = CASE WHEN @RelatedTableCount > 1 THEN @TargetColumnList ELSE @SourceColumnList END;
		SET @UpdateColumns = (	SELECT string_agg ('t.' + c.value + '=' + @TablePrefix + '.' + c.value  , ',') FROM string_split(@UpdateColumnList,',')  c WHERE  
						c.value not in (SELECT value from string_split(@BusinessKeys,','))
						AND c.value NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))
						AND c.value NOT IN (SELECT ltrim(value) from string_split(@LineageColumns,','))
						AND c.value <> @PrimaryKey
		)
		
			   
		/* Add Rowchecksum */
		SELECT @NonKeyColumns = (select string_agg(value,',') from string_split(@SourceColumnList, ',') c 
								WHERE c.value <> @PrimaryKey
								AND c.value not in (SELECT value from string_split(@BusinessKeys,','))
								AND c.value NOT IN (SELECT ltrim(value) from string_split(@LineageColumns,','))
								AND c.value NOT IN (SELECT ltrim(value) from string_split(@HideColumns,',')))
			, @NonKeyColumnsQualified = (select string_agg( @TablePrefix + '.' + value,',') from string_split(@SourceColumnList, ',') c 
								WHERE c.value <> @PrimaryKey
								AND c.value not in (SELECT value from string_split(@BusinessKeys,','))
								AND c.value NOT IN (SELECT ltrim(value) from string_split(@LineageColumns,','))
								AND c.value NOT IN (SELECT ltrim(value) from string_split(@HideColumns,',')))



		IF @RowChecksum =1
		BEGIN
			SELECT @InsertColumns = @InsertColumns + ',RowChecksum'
			SELECT @SelectColumns = @SelectColumns + ',ISNULL(binary_checksum(' + coalesce(@NonKeyColumnsQualified,'''''') + '), 0) AS RowChecksum'	
			SELECT @UpdateColumns = @UpdateColumns + ',t.RowChecksum=ISNULL(binary_checksum(' + coalesce(@NonKeyColumnsQualified,'''''') + '), 0)'
		END
		IF  @DedupeFlag=1 and @RowVersionNoFlag =0 
			SET @SelectColumns = @SelectColumns + ',ROW_NUMBER() OVER (PARTITION BY ' + @BusinessKeysQualified + ' ORDER BY ' + @DedupeOrderBy +  ') as RowVersionNo'

		IF @PrestageTargetFlag=1 AND object_id(@TargetObject) IS NOT NULL and  @Skip=0	
		BEGIN
			SELECT @CTAS=1,  @TargetObjectFinal=@TargetObject
			SET @TargetObject=@PrestageTargetObject
		END

		/* SchemaDrift*/
		IF @SchemaDrift=1 AND @AutoDrop=0 and  @TargetObjectID IS NOT NULL and  @Skip=0	
		BEGIN
			 EXEC [dwa].[usp_TableLoad_CheckSchemaDrift] @SourceObjectID=@SourceObjectID, @TargetObjectID=@TargetObjectID,@PrimaryKey=@PrimaryKey, @BusinessKeys =@BusinessKeys, @RelatedTables=@RelatedTables,@RowChecksum=@Rowchecksum,@NonKeyColumns=@NonKeyColumns, @IsSchemaDrift = @IsSchemaDrift OUTPUT		
			 IF @IsSchemaDrift = 1
				EXEC [dwa].[usp_TableLoad_DeploySchemaDrift] @TableID = @TableID
		END	
	
		/*CTAS*/
		IF @CTAS =1 and @Skip=0		
		BEGIN
			DECLARE @ctasTarget sysname =@TargetObject
			if @TableSwapFlag =1
				SET @ctasTarget = REPLACE(@TargetObject, '.','_int.')
			EXEC [dwa].[usp_TableLoad_CTAS] @TableID = @TableID, @TargetObject=@ctasTarget,@SourceObject=@SourceObject ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL,@SqlWhere=@SqlWhere,@PrestageJoinSQL=@PrestageJoinSQL,@DeltaJoinSql=@DeltaJoinSql,@RelatedBusinessKeys=@RelatedBusinessKeys,@SqlWhereOuter=@SqlWhereOuter, @InsertColumns =@InsertColumns,@TablePrefix=@TablePrefix,@PrestageTargetFlag=@PrestageTargetFlag ,@DeleteDDL=@DeleteDDL ,@RowChecksum=@RowChecksum 
			IF @PrestageTargetFlag=1 
				SELECT @sqlWhereOuter=null, @CTAS =0, @JoinSQL=NULL, @SqlWhere=null, @SourceObject=@PrestageTargetObject, @TargetObject=@TargetObjectFinal
			ELSE
				SELECT @InsertFlag=0, @UpdateFlag=0,@DeleteFlag=0
		END
			

		ELSE IF @Skip=0
		BEGIN
			IF OBJECT_ID(@TargetObject) IS NULL
				EXEC [dwa].usp_TableCreate @TableID;

			IF @DeleteDDL IS NOT NULL
				SET @SqlWhere = coalesce(@SqlWhere + ' AND ', 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'			
		END 	

		/*INSERT*/
		IF @InsertFlag=1
			EXEC [dwa].[usp_TableLoad_Insert] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@InsertColumns=@InsertColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists,@TablePrefix=@TablePrefix,@BusinessKeys=@BusinessKeys,@PrestageTargetFlag=@PrestageTargetFlag,@PrestageTargetObject=@PrestageTargetObject,@SqlWhereOuter=@SqlWhereOuter

				
		/*UPDATE*/
		IF @UpdateFlag =1 and @Exists=1 and @CTAS =0  and @Skip=0	
		BEGIN
			IF @DeleteDDL is not null 				
				SET @SqlWhere ='WHERE NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'
				EXEC [dwa].[usp_TableLoad_Update] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@UpdateColumns=@UpdateColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists,@TablePrefix=@TablePrefix,@BusinessKeys=@BusinessKeys, @PrestageTargetFlag=@PrestageTargetFlag, @SqlWhereOuter=@SqlWhereOuter	
		END	

		/*DELETE*/
		IF coalesce(@DeleteFlag,1) =1 and @Exists=1 and @DeleteDDL is not null and @Skip=0
			EXEC [dwa].[usp_TableLoad_Delete] @TableID=@TableID ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL 
	
		/* TableSwap */

		IF @TableSwapFlag=1 
		BEGIN
			exec [dwa].[usp_TableSwap] @SourceTable=@ctasTarget , @TargetTable=@TargetObject
		END
	END
	ELSE IF @Skip=0
		RAISERROR ('Unsupported SourceType=%s. Check Meta.config.edwTables.SourceType',16,1,@SourceType)

		IF @PostLoadSQL IS NOT NULL
		BEGIN
			PRINT '--PostLoadSQL Event'
			PRINT @PostLoadSQL
			PRINT ''
			EXEC (@PostLoadSQL)
		END
	
	END TRY

	BEGIN CATCH
		DECLARE @ErrorNumber INT, @ErrorProcedure varchar(128), @ErrorMessage varchar(4000)
		SELECT @ErrorNumber = ERROR_NUMBER(), @ErrorProcedure = ERROR_PROCEDURE(), @ErrorMessage = ERROR_MESSAGE();
		RAISERROR ('Error in %s object. Check error message ''%s''', 16, 1, @ErrorProcedure, @ErrorMessage)
	END CATCH
END