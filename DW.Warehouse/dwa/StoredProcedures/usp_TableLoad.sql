/*
Load Table into DWH with "smart" loading: selection of SELECT INTO v INSERT INTO ,DDL Changes, FK Maintenance
		Assumes view has correct Semantic Columns like RowcheckSum
Used By: LoadWorker ADF Pipeline
Example:
	exec [dwa].[usp_TableLoad] NULL,1,NULL
	[dwa].[usp_TableLoad] 'aw.DimDate' 
	[dwa].[usp_TableLoad] 'aw.FactFinance'

History:
	20/02/2025 Created
*/
CREATE     PROC [dwa].[usp_TableLoad] @TargetObject [nvarchar](512) =NULL, @TableID [int]= NULL, @RunID uniqueidentifier = NULL AS
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
		, @DedupeRows bit		 /* 1 if We Dedupe Rows by using RowVersionNo =1*/
		, @sqlWhere varchar(4000) /* Additional Where Clause predicates */
		, @HideColumns varchar(4000) /* Array of Columns to Hide */
		, @PrestageSourceFlag bit /* 1 if Prestage Source View to Staging table */
		, @PrestageTargetObject varchar(128) /* Full Name of Final Target Table if prestaging */
		, @PrestageTargetFlag bit			/* 1 if Target Table is a prestaged and needs DeltaAction 'N,X,C' to track New, Delete, Change */
		, @PrestageJoinSQL varchar(4000)	 /* Join Criteria for Prestaging */
		, @PrestageSchema varchar(128) 
		, @PrestageTable varchar(128)
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
		, @SchemaDrift bit
		, @RelatedTableCount smallint =0
		, @VarHideCols VARCHAR(4000) --Temp Variable to store hide columns value
		, @IsSchemaDrift bit
		, @IsIncremental bit;

	IF @TableID IS NULL
		SELECT @TableID = t.TableID FROM Meta.config.edwTables t WHERE ((t.SchemaName + '.' +  t.TableName = @TargetObject) OR t.TableName =@TargetObject);

	SELECT @SchemaName =SchemaName, @TableName =TableName, @SourceObject =t.SourceObject  ,@TableType =t.TableType , @TargetSchema =t.SchemaName ,  @TargetTable =t.TableName,  @SourceType=t.SourceType,  @PrimaryKey=t.PrimaryKey, @BusinessKeys =t.BusinessKeys, @UpdateFlag=coalesce(t.UpdateFlag,1)
	, @TablePrefix= coalesce(t.TablePrefix , case when t.TableType='Fact' THEN 'f' ELSE lower(left(replace(t.TableName, 'Dim',''),1)) END)
	, @AutoDrop=coalesce (t.AutoDrop,0)
	, @AutoTruncate =coalesce(t.AutoTruncate,0)
	, @CTAS =t.CTAS
	, @DedupeRows = coalesce(t.DedupeRows,0)
	, @PrestageSourceFlag =coalesce(t.PrestageSourceFlag,0)
	, @PrestageTargetFlag=coalesce(t.PrestageTargetFlag,0)
	, @PrestageSchema = coalesce(t.PrestageSchema,'stg')
	, @PrestageTable =coalesce(t.PrestageTable, t.TableName)
	, @PrestageTargetObject =coalesce(t.PrestageSchema,'stg') + '.'+ coalesce(t.PrestageTable, t.TableName)
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
	, @SchemaDrift =case when t.InsertFlag =0 then 0 else coalesce(t.SchemaDrift,1) end 
	, @IsIncremental = coalesce(t.IsIncremental,0)
	FROM Meta.config.edwTables t 
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

	IF @AutoDrop=1
		SELECT @UpdateFlag =0, @DeleteFlag=0, @SchemaDrift=0
    
	IF @TableName is null
	BEGIN
		SET @TargetObject=coalesce(@TargetObject, convert(varchar(10),@TableID))
		RAISERROR ('No meta data found in edwTables for %s',16,1,@TargetObject)
	END
	SET @CTAS= CASE WHEN (object_id(@TargetObject) is null or @AutoDrop=1) AND coalesce(@CTAS,1)=1 THEN 1 ELSE 0 END 

	IF object_id(@TargetObject) IS NOT NULL AND @IsIncremental = 1
		SELECT @AutoDrop = 0, @AutoTruncate=0,@InsertFlag=1,@CTAS=0

	IF object_id(@TargetObject) IS NULL OR (@PrestageTargetFlag=1 AND @AutoDrop=0) 
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

	IF @DedupeRows =1 and @skip=0
	BEGIN
		
		IF not exists (Select * From sys.columns c where c.name='RowVersionNo' AND c.object_id=object_id(@SourceObject))
		raiserror ('Table %s, Source %s must contain column [RowVersionNo] if DedupeRows=1 in Meta.config.edwTables.',16,1,@TargetObject, @SourceObject)
		SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ', 'WHERE ') +@TablePrefix + '.RowVersionNo=1' 
	END
	   
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
		IF @TableType='Dim' and Coalesce(@UpdateFlag,1) =1
			IF NOT EXISTS (SELECT * from sys.columns c WHERE object_id=object_id(@SourceObject) and c.name='RowChecksum')				
				raiserror('View ''%s'' is missing column RowChecksum, add this to View Definition. Either Checksum on non keys, or set UpdateFlag=0 if not updates in Meta.config.edwTables',16,1,@SourceObject)
		IF @TableType='Fact' and Coalesce(@UpdateFlag,1) =1
			IF NOT EXISTS (SELECT * from sys.columns c WHERE object_id=object_id(@SourceObject) and c.name='RowChecksum') 
				SET @UpdateFlag=0;
		
		IF @DeDupeRows=1
			SET @HideColumns =coalesce(@HideColumns + ',','')  + 'RowVersionNo';

		;WITH r as (
			SELECT r.TableID AS RelatedTableID
				, coalesce(j.JoinSQL, string_agg(@TablePrefix + '.' + ltrim(bk.value) + '=' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(bk.value), ' AND ')) fk
				, r.BusinessKeys, r.SchemaName, r.TableName, r.PrimaryKey
				, min(coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1)))) TablePrefix
				, min(coalesce(j.JoinSQL, 't.' + ltrim(r.PrimaryKey) + '=' + coalesce(r.TablePrefix, lower(left(replace(r.TableName, 'Dim', ''), 1))) + '.' + ltrim(r.PrimaryKey))) WhereJoinSQL
				, coalesce(j.JoinType, @JoinType) AS JoinType
			FROM Meta.config.edwTables r
			INNER JOIN Meta.config.edwTableJoins j ON j.RelatedTableID = r.TableID
			CROSS APPLY string_split(r.BusinessKeys, ',') bk
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
		, @RelatedTableCount=count(*)
		FROM Meta.config.edwTableJoins  j 
		INNER JOIN r on j.RelatedTableID =r.RelatedTableID
		WHERE j.TableID=@TableID
		
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

		/* SchemaDrift*/
		IF @SchemaDrift=1 AND COALESCE(@AutoDrop,0)=0 and  object_id(@TargetObject) IS NOT NULL
		BEGIN
			 EXEC [dwa].[usp_TableLoad_CheckSchemaDrift] @TableID=@TableID,@IsSchemaDrift = @IsSchemaDrift OUTPUT		
			 IF @IsSchemaDrift = 1
				EXEC [dwa].[usp_TableLoad_DeploySchemaDrift] @TableID = @TableID
		END	

		IF @InsertColumns IS NOT NULL
		SET @HideColumns = coalesce(@HideColumns+ ',' ,'') + @InsertColumns

		IF EXISTS (SELECT * FROM Meta.config.edwTableJoins WHERE TableID=@TableID and (ShowBK is null or ShowBK=0))
		BEGIN 	
		SELECT @VarHideCols = (select string_agg(r.BusinessKeys, ',') 
			from Meta.config.edwTableJoins j
			inner join Meta.config.edwTables r on r.TableID=j.RelatedTableID
			where j.TableID=@TableID
			and (ShowBK is null or ShowBK=0))
		SET @HideColumns = coalesce(@HideColumns+ ',' ,'') +@VarHideCols				
		END

		SELECT @PrimaryKey = coalesce(@PrimaryKey, replace(@TargetTable, 'Dim','') + 'Key')
		,@InsertColumns = case when @InsertColumns is null then '' else @InsertColumns + ',' end + (SELECT string_agg( c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject) and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))   )
		,@SelectColumns = case when @SelectColumns is null then '' else @SelectColumns + ',' end + (SELECT string_agg(  @TablePrefix + '.' + c.name, ',') FROM sys.columns c WHERE c.object_id = object_id(@SourceObject)  and c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns ,',')  ) )

		IF @PrestageSourceFlag=1
			BEGIN
				exec [dwa].[usp_TableLoad_PrestageView] @TargetObject = @TargetObject,@TableID =@TableID
				SET @SourceObject = @PrestageTargetObject +'_staging'
			END			
		PRINT '/*' + char(13) + char(10) + 'exec dwa.usp_TableLoad @TargetObject=''' + @TargetObject+''',@TableID=' + convert(varchar, @TableID) + ',@RunID=''' + convert (varchar(255), @RunID) +'''' +  char(13) + char(10) + '*/' ;
		
		IF @PrestageTargetFlag=1 AND object_id(@TargetObject) IS NOT NULL 
		BEGIN
			SET @CTAS=0 ; SET @Skip =1 ;			
			IF @LocalJoinSQL IS NOT NULL
				BEGIN 		
					SET @PrestageJoinSQL ='LEFT JOIN ' + @TargetObject + ' t ON '
					SET @PrestageJoinSQL = @PrestageJoinSQL + (SELECT string_agg(rtrim(bk.value), ' AND ') FROM string_split(rtrim(@LocalJoinSQL), ' ') bk)
				END
			 ELSE	
				BEGIN
					SET @PrestageJoinSQL ='LEFT JOIN ' + @TargetObject + ' t ON '
					SET @PrestageJoinSQL = @PrestageJoinSQL + (SELECT string_agg(@TablePrefix + '.' + ltrim(bk.value) +'=t.' + ltrim(bk.value),' AND ') FROM string_split(@BusinessKeys, ',') bk)  
				END

			SET @InsertFlag =1;	SET @AutoDrop=0	;			
			IF object_id(@PrestageTargetObject) IS NOT NULL 				
			BEGIN
				SET @sql ='DROP TABLE ' + @PrestageTargetObject
				PRINT @sql
				EXEC(@sql)	
			END					   
			BEGIN
				IF @PrestageJoinSQL is  null
					BEGIN
					SET @SelectColumns=@SelectColumns + ',''I'' as RowOperation'  
					IF @DeleteDDL is not null
						SET @sqlWhere= coalesce(@sqlWhere + char(13) + char(9) + ' AND ',char(13) + 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'  
					END
				ELSE
					BEGIN
					SET @SelectColumns=@SelectColumns + ',CASE ' + coalesce('WHEN ' + @TablePrefix + '.' + @DeleteDDL + ' THEN ''D''' ,'')  + ' WHEN t.RowChecksum is null then ''I'' WHEN t.RowChecksum <> ' + @TablePrefix + '.RowChecksum THEN ''U'' ELSE ''D'' END as RowOperation'  
					SET @SqlWhere=coalesce( @sqlWhere + char(13) +  ' AND ' , 'WHERE ') + '((t.RowChecksum is null OR t.RowChecksum <> ' + @TablePrefix + '.RowChecksum ' + coalesce(' AND NOT(' + @TablePrefix + '.' + @DeleteDDL+ ')','')   + ')' 
					IF @UpdateFlag=1 
						SET @sqlWhere= @sqlWhere + char(13) + char(9) +' OR (t.RowChecksum <> ' + @TablePrefix + '.RowChecksum)'
					IF @DeleteDDL is not null
						SET @sqlWhere= @sqlWhere +char(13) + char(9) + ' OR (' + @TablePrefix + '.' + @DeleteDDL + ' AND t.RowChecksum is not null)'
					SET @sqlWhere= @sqlWhere+ ')'
					END
			END
			
			/*PrestageTable - CTAS*/
			EXEC [dwa].[usp_TableLoad_CTAS] @TableID = @TableID, @TargetObject=@PrestageTargetObject,@SourceObject=@SourceObject ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL,@SqlWhere=@SqlWhere,@PrestageJoinSQL=@PrestageJoinSQL,@DeltaJoinSql=@DeltaJoinSql,@RelatedBusinessKeys=@RelatedBusinessKeys	
		END

		IF @RelatedTableCount >1
				SET @UpdateColumns = (	SELECT string_agg ('t.' + c.name + '=' + @TablePrefix + '.' + c.name  , ',') FROM sys.columns c WHERE c.is_identity=0 AND c.object_id = object_id(@TargetObject) 
								AND c.name not in (SELECT value from string_split(@BusinessKeys,','))
							    AND c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,','))
				)
		ELSE
				SET @UpdateColumns = (	SELECT string_agg ('t.' + c.name + '=' + @TablePrefix + '.' + c.name  , ',') FROM sys.columns c WHERE c.is_identity=0 AND c.object_id = object_id(@SourceObject) 
								AND c.name not in (SELECT value from string_split(@BusinessKeys,','))
								AND c.name NOT IN (SELECT ltrim(value) from string_split(@HideColumns,',')))

		IF @PrestageTargetFlag=1 AND object_id(@TargetObject) IS NOT NULL 
		BEGIN 	
			EXEC [dwa].[usp_TableLoad_Insert] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@PrestageTargetObject,@InsertColumns=@InsertColumns,@SelectColumns=@SelectColumns, @JoinSQL=NULL, @SqlWhere=NULL, @WhereJoinSQL= NULL, @Exists=@Exists;
			EXEC [dwa].[usp_TableLoad_Update] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@PrestageTargetObject,@UpdateColumns=@UpdateColumns,@SelectColumns=@SelectColumns, @JoinSQL=NULL, @SqlWhere=@Sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists;		
		END
		
		/*CTAS*/
		IF @CTAS =1 and @Skip=0		
			EXEC [dwa].[usp_TableLoad_CTAS] @TableID = @TableID, @TargetObject=@TargetObject,@SourceObject=@SourceObject ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL,@SqlWhere=@SqlWhere,@PrestageJoinSQL=@PrestageJoinSQL,@DeltaJoinSql=@DeltaJoinSql,@RelatedBusinessKeys=@RelatedBusinessKeys;
		ELSE IF @Skip=0
		BEGIN
			IF OBJECT_ID(@TargetObject) IS NULL
				EXEC [dwa].usp_TableCreate @TableID;

			IF @DeleteDDL IS NOT NULL
				SET @SqlWhere = coalesce(@SqlWhere + ' AND ', 'WHERE ') + 'NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'			
			/*INSERT*/
			IF @InsertFlag=1
			BEGIN 		
				EXEC [dwa].[usp_TableLoad_Insert] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@InsertColumns=@InsertColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists
			END				
		END 	
				
		/*UPDATE*/
		IF coalesce(@UpdateFlag,1) =1 and @Exists=1 and @CTAS =0  and @Skip=0	
		BEGIN
			IF @DeleteDDL is not null 				
				SET @SqlWhere ='WHERE NOT (' + @TablePrefix + '.' + @DeleteDDL + ')'
			BEGIN 	
				EXEC [dwa].[usp_TableLoad_Update] @TableID=@TableID,@TargetObject= @TargetObject,@SourceObject=@SourceObject,@UpdateColumns=@UpdateColumns,@SelectColumns=@SelectColumns, @JoinSQL=@JoinSQL, @SqlWhere=@sqlwhere, @WhereJoinSQL= @WhereJoinSQL, @Exists=@Exists
			END				
		END	

		/*DELETE*/
		IF coalesce(@DeleteFlag,1) =1 and @Exists=1 and @DeleteDDL is not null and @Skip=0
			EXEC [dwa].[usp_TableLoad_Delete] @TableID=@TableID ,@SelectColumns=@SelectColumns,@JoinSQL=@JoinSQL 

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