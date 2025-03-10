/****** Object:  Schema [audit]    Script Date: 07/03/2025 15:58:59 ******/
CREATE SCHEMA [audit];
/****** Object:  Schema [config]    Script Date: 07/03/2025 15:58:59 ******/
CREATE SCHEMA [config];
/****** Object:  Schema [devops]    Script Date: 07/03/2025 15:58:59 ******/
CREATE SCHEMA [devops];
/****** Object:  Table [dbo].[dict_artefacts]    Script Date: 07/03/2025 15:58:59 ******/
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
 
CREATE TABLE [dbo].[dict_artefacts](
	[id] [nvarchar](max) NULL,
	[display_name] [nvarchar](max) NULL,
	[description] [nvarchar](max) NULL,
	[type] [nvarchar](max) NULL,
	[workspace_id] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
 
/****** Object:  View [config].[Artefacts]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE View [config].[Artefacts] AS 
SELECT display_name as artefact, id as artefact_id, workspace_id , description
FROM [dbo].[dict_artefacts]
WHERE type in ('Notebook', 'DataPipeline');
 
/****** Object:  Table [audit].[LoadLog]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [audit].[LoadLog](
	[RunID] [uniqueidentifier] NULL,
	[TableID] [smallint] NOT NULL,
	[SchemaName] [varchar](128) NOT NULL,
	[TableName] [varchar](128) NOT NULL,
	[SourceObject] [varchar](128) NOT NULL,
	[SourceType] [varchar](10) NOT NULL,
	[StartDate] [date] NOT NULL,
	[StartDateTime] [datetime2](6) NOT NULL,
	[Status] [varchar](20) NOT NULL,
	[ErrorCode] [varchar](4000) NULL,
	[ErrorDescription] [varchar](4000) NULL,
	[ErrorSource] [varchar](4000) NULL
) ON [PRIMARY];
 
/****** Object:  Table [audit].[PipelineLog]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [audit].[PipelineLog](
	[LineageKey] [uniqueidentifier] NOT NULL,
	[RunID] [uniqueidentifier] NULL,
	[PipelineID] [int] NOT NULL,
	[PackageGroup] [varchar](50) NULL,
	[PipelineName] [varchar](512) NULL,
	[Status] [varchar](100) NOT NULL,
	[StartDate] [date] NOT NULL,
	[StartDateTime] [datetime2](6) NOT NULL,
	[EndDateTime] [datetime2](6) NULL,
	[Template] [varchar](200) NULL,
	[StartedBy] [varchar](512) NULL,
	[PipelineSequence] [smallint] NULL,
	[ErrorCode] [varchar](4000) NULL,
	[ErrorMessage] [varchar](4000) NULL,
	[Stage] [varchar](20) NULL,
	[Pipeline] [uniqueidentifier] NULL,
	[ParentRunID] [uniqueidentifier] NULL,
	[ParentGroupID] [uniqueidentifier] NULL,
	[WorkspaceID] [uniqueidentifier] NULL,
	[MonitoringUrl] [varchar](512) NULL,
	[RowsRead] [bigint] NULL,
	[CopyDuration] [int] NULL,
	[DataRead] [bigint] NULL,
	[DataWritten] [bigint] NULL,
	[ExtendedLog] [varchar](8000) NULL,
	[Debu utput] [varchar](8000) NULL,
 CONSTRAINT [PK_PipelineLog] PRIMARY KEY CLUSTERED 
(
	[LineageKey] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [audit].[ReleaseLog]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [audit].[ReleaseLog](
	[ReleaseNo] [int] NOT NULL,
	[SourceWorkspaceId] [uniqueidentifier] NOT NULL,
	[TargetWorkspaceId] [uniqueidentifier] NOT NULL,
	[SourceWorkspace] [varchar](50) NOT NULL,
	[TargetWorkspace] [varchar](50) NOT NULL,
	[ArtefactsJson] [varchar](8000) NULL,
	[SqlScripts] [varchar](8000) NULL,
	[ReleaseFolder] [varchar](8000) NOT NULL,
	[CreatedBy] [varchar](8000) NULL,
	[CreatedDateTime] [datetime2](6) NOT NULL,
	[Status] [varchar](20) NOT NULL,
	[ErrorMessage] [varchar](8000) NULL,
	[StartedTime] [datetime2](6) NULL,
	[CompletedTime] [datetime2](6) NULL,
	[ReleaseNotes] [varchar](8000) NULL,
	[ReleaseNotesFile] [varchar](500) NULL,
PRIMARY KEY CLUSTERED 
(
	[ReleaseNo] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[Configurations]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[Configurations](
	[ConfigurationID] [int] NOT NULL,
	[ConfigurationName] [varchar](8000) NULL,
	[ConnectionSettings] [varchar](8000) NULL,
	[Enabled] [int] NULL,
 CONSTRAINT [PK_Configurations] PRIMARY KEY CLUSTERED 
(
	[ConfigurationID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[DatasetLineage]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[DatasetLineage](
	[Dataset] [varchar](50) NOT NULL,
	[DatasetTableName] [varchar](128) NOT NULL,
	[SourceDatabase] [varchar](128) NULL,
	[SourceObject] [varchar](255) NULL,
	[SourceType] [varchar](50) NULL,
	[SourceConnection] [varchar](255) NULL,
	[PipelineID] [int] NULL
) ON [PRIMARY];
 
/****** Object:  Table [config].[Datasets]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[Datasets](
	[Dataset] [varchar](50) NOT NULL,
	[Monitored] [bit] NULL,
	[MaxAgeHours] [int] NULL,
	[PackageGroup] [varchar](50) NULL,
	[ConfigurationID] [int] NULL,
	[DataOwner] [varchar](255) NULL,
 CONSTRAINT [PK_Datasets] PRIMARY KEY CLUSTERED 
(
	[Dataset] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[edwTableJoins]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[edwTableJoins](
	[TableID] [smallint] NOT NULL,
	[RelatedTableID] [smallint] NOT NULL,
	[ShowBK] [bit] NULL,
	[JoinSQL] [varchar](512) NULL,
	[AliasPK] [varchar](50) NULL,
	[JoinOrder] [smallint] NULL,
	[JoinType] [varchar](20) NULL,
 CONSTRAINT [PK_edwTableJoins] PRIMARY KEY CLUSTERED 
(
	[TableID] ASC,
	[RelatedTableID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[edwTableLineage]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[edwTableLineage](
	[TableID] [int] NULL,
	[SourceTable] [varchar](128) NULL,
	[SourceType] [varchar](128) NULL,
	[SourceDatabase] [varchar](128) NULL,
	[PipelineID] [int] NULL
) ON [PRIMARY];
 
/****** Object:  Table [config].[edwTables]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[edwTables](
	[TableID] [smallint] NOT NULL,
	[SchemaName] [varchar](128) NULL,
	[TableName] [varchar](128) NULL,
	[LoadDescription] [varchar](512) NULL,
	[TableType] [varchar](20) NULL,
	[SourceObject] [varchar](8000) NULL,
	[SourceType] [varchar](20) NULL,
	[BusinessKeys] [varchar](512) NULL,
	[PrimaryKey] [varchar](512) NULL,
	[DefaultPackageGroup] [varchar](50) NULL,
	[TablePrefix] [varchar](10) NULL,
	[Enabled] [bit] NULL,
	[InsertFlag] [bit] NULL,
	[UpdateFlag] [bit] NULL,
	[DeleteFlag] [bit] NULL,
	[DedupeRows] [bit] NULL,
	[AutoDrop] [bit] NULL,
	[CTAS] [bit] NULL,
	[AutoTruncate] [bit] NULL,
	[DeleteDDL] [varchar](8000) NULL,
	[SkipSqlCondition] [varchar](8000) NULL,
	[PrestageSourceFlag] [bit] NULL,
	[PrestageTargetFlag] [bit] NULL,
	[PrestageSchema] [varchar](8000) NULL,
	[PrestageTable] [varchar](8000) NULL,
	[DeltaTargetObject] [varchar](8000) NULL,
	[PreLoadSQL] [varchar](8000) NULL,
	[PostLoadSQL] [varchar](8000) NULL,
	[WhereSQL] [varchar](8000) NULL,
	[JoinType] [varchar](8000) NULL,
	[Language] [varchar](8000) NULL,
	[SchemaDrift] [bit] NULL,
	[IsIncremental] [bit] NULL,
	[SCD] [bit] NULL,
	[Identity] [bit] NULL,
 CONSTRAINT [PK_edwTables] PRIMARY KEY CLUSTERED 
(
	[TableID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PackageGroupLinks]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PackageGroupLinks](
	[PackageGroup] [varchar](50) NOT NULL,
	[ChildPackageGroup] [varchar](50) NOT NULL,
 CONSTRAINT [PK_PackageGroupLinks] PRIMARY KEY NONCLUSTERED 
(
	[PackageGroup] ASC,
	[ChildPackageGroup] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PackageGroups]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PackageGroups](
	[PackageGroup] [varchar](50) NOT NULL,
	[Monitored] [bit] NULL,
	[SortOrder] [varchar](50) NULL,
 CONSTRAINT [PK_PackageGroups] PRIMARY KEY NONCLUSTERED 
(
	[PackageGroup] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PackageGroupTables]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PackageGroupTables](
	[PackageGroup] [varchar](50) NOT NULL,
	[TableID] [smallint] NOT NULL,
 CONSTRAINT [PK_PackageGroupTables] PRIMARY KEY CLUSTERED 
(
	[PackageGroup] ASC,
	[TableID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PipelineGroups]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PipelineGroups](
	[PipelineGroupID] [int] NOT NULL,
	[PipelineGroupName] [varchar](200) NULL,
	[PipelineGroupDesc] [varchar](500) NULL,
	[Template] [varchar](200) NULL,
	[SourceSettings] [varchar](8000) NULL,
	[TargetSettings] [varchar](8000) NULL,
	[Enabled] [int] NULL,
	[ActivitySettings] [varchar](8000) NULL,
	[SourceConfigurationID] [int] NULL,
	[TargetConfigurationID] [int] NULL,
	[PackageGroup] [varchar](50) NULL,
	[Stage] [varchar](8000) NULL,
	[PostExecuteSQL] [varchar](8000) NULL,
	[PreExecuteSQL] [varchar](8000) NULL,
	[SourceConnectionSettings] [varchar](8000) NULL,
	[TargetConnectionSettings] [varchar](8000) NULL,
	[ContinueOnError] [varchar](200) NULL,
 CONSTRAINT [PK_PipelineGroups] PRIMARY KEY NONCLUSTERED 
(
	[PipelineGroupID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PipelineMeta]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PipelineMeta](
	[PipelineID] [int] NULL,
	[Pipeline] [varchar](200) NULL,
	[Template] [varchar](128) NULL,
	[SourceConnectionSettings] [varchar](8000) NULL,
	[TargetConnectionSettings] [varchar](8000) NULL,
	[SourceSettings] [varchar](8000) NULL,
	[TargetSettings] [varchar](8000) NULL,
	[ActivitySettings] [varchar](8000) NULL,
	[PipelineSequence] [int] NOT NULL,
	[PackageGroup] [varchar](8000) NULL,
	[PreExecuteSQL] [varchar](8000) NULL,
	[PostExecuteSQL] [varchar](8000) NULL,
	[Enabled] [bit] NOT NULL,
	[Stage] [varchar](50) NULL,
	[TemplateType] [varchar](50) NULL,
	[TemplateID] [uniqueidentifier] NULL,
	[TableID] [int] NULL
) ON [PRIMARY];
 
/****** Object:  Table [config].[PipelineMetaCache]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PipelineMetaCache](
	[ChecksumToken] [int] NULL
) ON [PRIMARY];
 
/****** Object:  Table [config].[Pipelines]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[Pipelines](
	[PipelineID] [int] NOT NULL,
	[Pipeline] [varchar](200) NULL,
	[PipelineGroupID] [int] NULL,
	[PipelineSequence] [int] NULL,
	[SourceSettings] [varchar](8000) NULL,
	[TargetSettings] [varchar](8000) NULL,
	[ActivitySettings] [varchar](8000) NULL,
	[Enabled] [int] NULL,
	[PackageGroup] [varchar](50) NULL,
	[TableID] [smallint] NULL,
	[Stage] [varchar](50) NULL,
	[PostExecuteSQL] [varchar](8000) NULL,
	[Template] [varchar](200) NULL,
	[PreExecuteSQL] [varchar](8000) NULL,
	[SourceConnectionSettings] [varchar](8000) NULL,
	[TargetConnectionSettings] [varchar](8000) NULL,
	[SourceConfigurationID] [int] NULL,
	[TargetConfigurationID] [int] NULL,
	[ContinueOnError] [varchar](8000) NULL,
	[JoinSQL] [varchar](8000) NULL,
	[Comments] [varchar](200) NULL,
 CONSTRAINT [PK_Pipelines] PRIMARY KEY CLUSTERED 
(
	[PipelineID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[PipelineTables]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[PipelineTables](
	[PipelineID] [int] NOT NULL,
	[TableID] [smallint] NOT NULL,
	[TargetSchema] [varchar](128) NULL,
	[TargetTable] [varchar](128) NULL,
	[TargetDatabase] [varchar](128) NULL,
 CONSTRAINT [PK_PipelineTables] PRIMARY KEY NONCLUSTERED 
(
	[PipelineID] ASC,
	[TableID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
/****** Object:  Table [config].[Templates]    Script Date: 07/03/2025 15:58:59 ******/
 
CREATE TABLE [config].[Templates](
	[Template] [varchar](200) NOT NULL,
	[TemplateDescription] [varchar](500) NULL,
	[SourceSettings] [varchar](8000) NULL,
	[TargetSettings] [varchar](8000) NULL,
	[ArtefactType] [varchar](100) NULL,
 CONSTRAINT [PK_Templates] PRIMARY KEY NONCLUSTERED 
(
	[Template] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY];
 
ALTER TABLE [audit].[PipelineLog]  WITH CHECK ADD  CONSTRAINT [FK_PipelineLog_PackageGroups] FOREIGN KEY([PackageGroup])
REFERENCES [config].[PackageGroups] ([PackageGroup]);
 
ALTER TABLE [audit].[PipelineLog] CHECK CONSTRAINT [FK_PipelineLog_PackageGroups];
 
ALTER TABLE [audit].[PipelineLog]  WITH CHECK ADD  CONSTRAINT [FK_PipelineLog_Pipelines] FOREIGN KEY([PipelineID])
REFERENCES [config].[Pipelines] ([PipelineID]);
 
ALTER TABLE [audit].[PipelineLog] CHECK CONSTRAINT [FK_PipelineLog_Pipelines];
 
ALTER TABLE [config].[DatasetLineage]  WITH CHECK ADD  CONSTRAINT [FK_DatasetLineage_Datasets] FOREIGN KEY([Dataset])
REFERENCES [config].[Datasets] ([Dataset]);
 
ALTER TABLE [config].[DatasetLineage] CHECK CONSTRAINT [FK_DatasetLineage_Datasets];
 
ALTER TABLE [config].[DatasetLineage]  WITH CHECK ADD  CONSTRAINT [FK_DatasetLineage_Pipelines] FOREIGN KEY([PipelineID])
REFERENCES [config].[Pipelines] ([PipelineID]);
 
ALTER TABLE [config].[DatasetLineage] CHECK CONSTRAINT [FK_DatasetLineage_Pipelines];
 
ALTER TABLE [config].[Datasets]  WITH CHECK ADD  CONSTRAINT [FK_Datasets_Configurations] FOREIGN KEY([ConfigurationID])
REFERENCES [config].[Configurations] ([ConfigurationID]);
 
ALTER TABLE [config].[Datasets] CHECK CONSTRAINT [FK_Datasets_Configurations];
 
ALTER TABLE [config].[edwTableJoins]  WITH CHECK ADD  CONSTRAINT [FK_edwTableJoins_edwTables] FOREIGN KEY([TableID])
REFERENCES [config].[edwTables] ([TableID]);
 
ALTER TABLE [config].[edwTableJoins] CHECK CONSTRAINT [FK_edwTableJoins_edwTables];
 
ALTER TABLE [config].[edwTableJoins]  WITH CHECK ADD  CONSTRAINT [FK_edwTableJoins_edwTables1] FOREIGN KEY([RelatedTableID])
REFERENCES [config].[edwTables] ([TableID]);
 
ALTER TABLE [config].[edwTableJoins] CHECK CONSTRAINT [FK_edwTableJoins_edwTables1];
 
ALTER TABLE [config].[PackageGroupLinks]  WITH CHECK ADD  CONSTRAINT [FK_PackageGroupLinks_PackageGroups] FOREIGN KEY([PackageGroup])
REFERENCES [config].[PackageGroups] ([PackageGroup]);
 
ALTER TABLE [config].[PackageGroupLinks] CHECK CONSTRAINT [FK_PackageGroupLinks_PackageGroups];
 
ALTER TABLE [config].[PackageGroupLinks]  WITH CHECK ADD  CONSTRAINT [FK_PackageGroupLinks_PackageGroups1] FOREIGN KEY([ChildPackageGroup])
REFERENCES [config].[PackageGroups] ([PackageGroup]);
 
ALTER TABLE [config].[PackageGroupLinks] CHECK CONSTRAINT [FK_PackageGroupLinks_PackageGroups1];
 
ALTER TABLE [config].[PackageGroupTables]  WITH CHECK ADD  CONSTRAINT [FK_PackageGroupTables_edwTables] FOREIGN KEY([TableID])
REFERENCES [config].[edwTables] ([TableID]);
 
ALTER TABLE [config].[PackageGroupTables] CHECK CONSTRAINT [FK_PackageGroupTables_edwTables];
 
ALTER TABLE [config].[PackageGroupTables]  WITH CHECK ADD  CONSTRAINT [FK_PackageGroupTables_PackageGroups] FOREIGN KEY([PackageGroup])
REFERENCES [config].[PackageGroups] ([PackageGroup]);
 
ALTER TABLE [config].[PackageGroupTables] CHECK CONSTRAINT [FK_PackageGroupTables_PackageGroups];
 
ALTER TABLE [config].[PipelineGroups]  WITH CHECK ADD  CONSTRAINT [FK_PipelineGroups_Configurations] FOREIGN KEY([SourceConfigurationID])
REFERENCES [config].[Configurations] ([ConfigurationID]);
 
ALTER TABLE [config].[PipelineGroups] CHECK CONSTRAINT [FK_PipelineGroups_Configurations];
 
ALTER TABLE [config].[PipelineGroups]  WITH CHECK ADD  CONSTRAINT [FK_PipelineGroups_Configurations1] FOREIGN KEY([TargetConfigurationID])
REFERENCES [config].[Configurations] ([ConfigurationID]);
 
ALTER TABLE [config].[PipelineGroups] CHECK CONSTRAINT [FK_PipelineGroups_Configurations1];
 
ALTER TABLE [config].[PipelineGroups]  WITH CHECK ADD  CONSTRAINT [FK_PipelineGroups_Templates] FOREIGN KEY([Template])
REFERENCES [config].[Templates] ([Template]);
 
ALTER TABLE [config].[PipelineGroups] CHECK CONSTRAINT [FK_PipelineGroups_Templates];
 
ALTER TABLE [config].[Pipelines]  WITH CHECK ADD  CONSTRAINT [FK_Pipelines_Configurations] FOREIGN KEY([SourceConfigurationID])
REFERENCES [config].[Configurations] ([ConfigurationID]);
 
ALTER TABLE [config].[Pipelines] CHECK CONSTRAINT [FK_Pipelines_Configurations];
 
ALTER TABLE [config].[Pipelines]  WITH CHECK ADD  CONSTRAINT [FK_Pipelines_PackageGroups] FOREIGN KEY([PackageGroup])
REFERENCES [config].[PackageGroups] ([PackageGroup]);
 
ALTER TABLE [config].[Pipelines] CHECK CONSTRAINT [FK_Pipelines_PackageGroups];
 
ALTER TABLE [config].[Pipelines]  WITH CHECK ADD  CONSTRAINT [FK_Pipelines_PipelineGroups] FOREIGN KEY([PipelineGroupID])
REFERENCES [config].[PipelineGroups] ([PipelineGroupID]);
 
ALTER TABLE [config].[Pipelines] CHECK CONSTRAINT [FK_Pipelines_PipelineGroups];
 
ALTER TABLE [config].[Pipelines]  WITH CHECK ADD  CONSTRAINT [FK_Pipelines_Templates] FOREIGN KEY([Template])
REFERENCES [config].[Templates] ([Template]);
 
ALTER TABLE [config].[Pipelines] CHECK CONSTRAINT [FK_Pipelines_Templates];
 
ALTER TABLE [config].[PipelineTables]  WITH CHECK ADD  CONSTRAINT [FK_PipelineTables_edwTables] FOREIGN KEY([TableID])
REFERENCES [config].[edwTables] ([TableID]);
 
ALTER TABLE [config].[PipelineTables] CHECK CONSTRAINT [FK_PipelineTables_edwTables];
 
ALTER TABLE [config].[PipelineTables]  WITH CHECK ADD  CONSTRAINT [FK_PipelineTables_Pipelines] FOREIGN KEY([PipelineID])
REFERENCES [config].[Pipelines] ([PipelineID]);
 
ALTER TABLE [config].[PipelineTables] CHECK CONSTRAINT [FK_PipelineTables_Pipelines];
 
/****** Object:  StoredProcedure [audit].[usp_LoadFailure]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Mark Load as Failed in case of Failure
Example:		exec [audit].[usp_LoadFailure] 2, 1, '1', NULL
				exec [audit].[usp_LoadFailure] '5CC853AC-BA18-438E-82A0-6621533B8728', 1, '1', 'Error Description 1'
History:		
		14/08/2023 Shruti, Created
*/
CREATE PROCEDURE [audit].[usp_LoadFailure] @RunID UNIQUEIDENTIFIER, @TableID [int], @ErrorCode [int], @ErrorDescription [nvarchar] (max) AS
BEGIN
	
	DECLARE @ExecutionKey INT
	DECLARE @ErrorCount INT
	DECLARE @ErrorCountLoad INT
	DECLARE @SourceObject NVARCHAR(4000)

	IF LEN(@ErrorDescription) = 0
		SELECT @ErrorDescription = 'Load Failed. Check Logs'
			, @ErrorCode = - 1

	INSERT INTO [audit].[LoadLog] (
		RunID
		, TableID
		, SourceObject
		, SchemaName
		, TableName
		, StartDate
		, StartDateTime
		, SourceType
		, Status
		, ErrorCode
		, ErrorDescription
		, ErrorSource
		)
	SELECT COALESCE(@RunID,NULL) AS RunID
		, t.TableID
		, t.SourceObject
		, t.SchemaName
		, t.TableName
		, GETDATE()
		, GETDATE()
		, t.SourceType
		, 'Failed'
		, @ErrorCode
		, @ErrorDescription
		, t.SourceObject
	FROM config.edwTables t
	WHERE t.TableID = @TableID
	
END;
 
/****** Object:  StoredProcedure [audit].[usp_LoadStart]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:Start Tracking a new Load 
Example:		
	exec [audit].[usp_LoadStart]  null,1
History:
	03/08/2023 Deepak, Created
	14/08/2023 Shruti, Refactoring
*/
CREATE PROC [audit].[usp_LoadStart] @RunID [uniqueidentifier], @TableID [int] AS
BEGIN
	
	SET @RunID =coalesce(@RunID,newid())
		INSERT INTO [audit].[LoadLog]
		(   RunID
		   ,TableID
		   ,SourceObject
		   ,SchemaName
		   ,TableName
		   ,StartDate
		   ,StartDateTime
		   ,SourceType
		   ,Status)	
		SELECT @RunID
		      ,t.TableID
		      ,t.SourceObject
		      ,t.SchemaName
		      ,t.TableName
		      ,getdate()
		      ,getdate()
		      ,t.SourceType
		      ,'Started'
		FROM config.edwTables t 
		WHERE t.TableID=@TableID
END;
 
/****** Object:  StoredProcedure [audit].[usp_LoadSuccess]    Script Date: 07/03/2025 15:58:59 ******/
 /*
Description:	Mark Load as Successful
Example:		exec [audit].[usp_LoadSuccess] NULL, 1 
History:		
		14/08/2023 Shruti, Created
*/
CREATE PROC [audit].[usp_LoadSuccess] @RunID uniqueidentifier, @TableID [int] AS
BEGIN
	
	INSERT INTO [audit].[LoadLog]
	(
		RunID
	   ,TableID
	   ,SourceObject
	   ,SchemaName
	   ,TableName
	   ,StartDate
	   ,StartDateTime
	   ,SourceType
	   ,Status
	)	
	SELECT COALESCE(@RunID,newId())	
		  ,t.TableID
		  ,t.SourceObject
		  ,t.SchemaName
		  ,t.TableName
		  ,GETDATE()
		  ,GETDATE()
		  ,t.SourceType
		  ,'Completed'
	FROM config.edwTables t 
	WHERE t.TableID=@TableID

END;
 
/****** Object:  StoredProcedure [audit].[usp_PipelineEndFailure]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Log Error for a Pipeline
Used by:		ADF Pipeline-Worker
Example:		
	 exec [audit].[usp_PipelineEndFailure] 'FED89103-A6A6-4DE9-95C8-72F7FFEB029F','ErrorCode','ErrorMessage'

History:
	22/08/2021 Bob, Ceeated for Fabric DWA
	04/06/2024 Aidan, Added Retry to deal with transient retry issue
	31/10/2024 Kristan, Changed to dwa schema
*/
CREATE  PROC [audit].[usp_PipelineEndFailure] @LineageKey uniqueidentifier,@ErrorCode [varchar](max),@ErrorMessage [varchar](max) AS
BEGIN
	 
	DECLARE  @RetryCount			int = 0
	        ,@MaxRetryCount			    int = 5
			,@Success					BIT = 0
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 	
			UPDATE audit.PipelineLog 
					SET Status='Failed', ErrorCode=coalesce(@ErrorCode,'Inner Activity'), ErrorMessage=coalesce(@ErrorMessage,'Inner Activiy Failed. Check Monitoring Hub '+ MonitoringUrl)
			WHERE LineageKey=@LineageKey and Status <> 'Failed'
			SET @Success = 1 
		END TRY
			BEGIN CATCH
			 IF ERROR_NUMBER() IN (24556) /* Update Concurrency Error */
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					DECLARE @BackoffDelay varchar(12) = RIGHT('00:00:' + CAST(LEAST(30.0, 0.1 * POWER(5, @RetryCount - 1)) AS VARCHAR(10)) + '00', 12)
					WAITFOR DELAY  @BackoffDelay
				 END
			  ELSE
				THROW
			 IF @RetryCount >@MaxRetryCount 
				THROW
		END CATCH 
	END

END;
 
/****** Object:  StoredProcedure [audit].[usp_PipelineEndSuccess]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Complete a Pipeline
Used By:		ADF Pipeline-Worker 

-- Example
	exec [audit].[usp_PipelineStart] 37, '7EEFA88E-D9F9-4204-AC47-97F2FC3362CE', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82', 2
	SELECT * FROM audit.PipelineLog ORDER BY StartDateTime DESC 
	exec audit.usp_PipelineEndSuccess 1,'7EEFA88E-D9F9-4204-AC47-97F2FC3362CE'
History:	
	30/04/2024 Bob, Migrated to Fabric
	04/06/2024 Aidan, Added Backoff Al rithm
	31/10/2024 Kristan, Changed schema to dwa
*/
CREATE PROC [audit].[usp_PipelineEndSuccess]  
	@LineageKey uniqueidentifier
AS
BEGIN
		
	DECLARE @PostExecuteSQL varchar(8000)
	DECLARE @DateTime datetime
	DECLARE @RetryCount INT =0
	DECLARE @MaxRetryCount INT =3
	DECLARE @Success    BIT =0

	SELECT @PostExecuteSQL = coalesce(p.PostExecuteSQL , pg.PostExecuteSQL)
	,@DateTime = GETDATE()
	FROM [config].[Pipelines] p 
	INNER JOIN [config].[PipelineGroups] pg on pg.PipelineGroupID =p.PipelineGroupID
			
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 
			UPDATE [audit].PipelineLog	
			SET 
				Status='Completed'
				, EndDateTime =@DateTime
			WHERE LineageKey = @LineageKey
			AND EndDateTime is null
			SET @Success = 1 
		END TRY
		BEGIN CATCH
			IF ERROR_NUMBER() IN (24556)
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					DECLARE @BackoffDelay varchar(12) = RIGHT('00:00:' + CAST(LEAST(30.0, 0.1 * POWER(5, @RetryCount - 1)) AS VARCHAR(10)) + '00', 12)
					WAITFOR DELAY  @BackoffDelay
				END
			ELSE
				THROW
			IF @RetryCount > @MaxRetryCount
				THROW
		END CATCH 
	END

	
	IF @PostExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Post Execute*/' + char(13) + @PostExecuteSQL)
		EXEC( @PostExecuteSQL )
	END	

END;
 
/****** Object:  StoredProcedure [audit].[usp_PipelineLogCopy]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Extended Logging for Copy Activity
Used by:		Extract-SQL and any Copy Activity
Example:		
	exec [audit].[usp_PipelineLogCopy] 'FED89103-A6A6-4DE9-95C8-72F7FFEB029F','{
	"dataRead": 594,
	"dataWritten": 1509,
	"filesWritten": 1,
	"sourcePeakConnections": 1,
	"sinkPeakConnections": 1,
	"rowsRead": 22,
	"rowsCopied": 22,
	"copyDuration": 15,
	"throughput": 0.074,
	"errors": [],
	"usedParallelCopies": 1,
	"executionDetails": [
		{
			"source": {
				"type": "SqlServer"
			},
			"sink": {
				"type": "Lakehouse"
			},
			"status": "Succeeded",
			"start": "5/2/2024, 10:18:28 AM",
			"duration": 15,
			"usedParallelCopies": 1,
			"profile": {
				"queue": {
					"status": "Completed",
					"duration": 0
				},
				"transfer": {
					"status": "Completed",
					"duration": 8,
					"details": {
						"readingFromSource": {
							"type": "SqlServer",
							"workingDuration": 0,
							"timeToFirstByte": 0
						},
						"writingToSink": {
							"type": "Lakehouse",
							"workingDuration": 0
						}
					}
				}
			},
			"detailedDurations": {
				"queuingDuration": 0,
				"timeToFirstByte": 0,
				"transferDuration": 8
			}
		}
	],
	"dataConsistencyVerification": {
		"VerificationResult": "NotVerified"
	}
}'

History:
	22/08/2021 Bob, Ceeated for Fabric DWA

*/
CREATE PROC [audit].[usp_PipelineLogCopy] @LineageKey uniqueidentifier,@Json varchar(8000)
AS
BEGIN
	 
	DECLARE @Status varchar(100)
	DECLARE @errors varchar(8000)
	DECLARE @RowCount bigint
	DECLARE @copyDuration int
	DECLARE @DataWritten bigint
	DECLARE @DataRead bigint
	DECLARE @ErrorCode varchar(4000)
	DECLARE @ErrorMessage varchar(4000)
	DECLARE @RetryCount INT =0
	DECLARE @MaxRetryCount INT =3
	DECLARE @Success    BIT =0

	SELECT @errors=value
	FROM OPENJSON (@Json ) t
	WHERE [key]='errors'

	IF @errors ='[]' 
		SET @Status='Completed'
	ELSE
	BEGIN
		SET @Status	='Failed'
		SELECT @ErrorCode = Code, @ErrorMessage =[Message]
		FROM OPENJSON (@errors) 
		WITH (
			Code varchar(4000) ,
			Message varchar(4000) 
		);
	END

	select @RowCount = rowsRead, @CopyDuration=copyDuration, @DataRead =dataRead, @DataWritten=dataWritten
	FROM OPENJSON (@Json) 
	WITH (
		dataRead int ,
		dataWritten int ,
		rowsRead bigint ,
		copyDuration int 
	);


	WHILE @Success = 0
	BEGIN
		BEGIN TRY 
			UPDATE audit.PipelineLog 
				SET [Status]=@Status, ErrorCode=@ErrorCode, ErrorMessage=@ErrorMessage, CopyDuration=@copyDuration, RowsRead=@RowCount, DataRead=@DataRead, DataWritten=@DataWritten, ExtendedLog=@Json
			WHERE LineageKey=@LineageKey
			SET @Success = 1
		END TRY
		BEGIN CATCH
			 IF ERROR_NUMBER() IN (24556) /* Update Concurrency Error */
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					WAITFOR DELAY '00:00:00.100'
					--INSERT INTO Audit.dbo.Dump
					--VALUES (@LineageKey, ERROR_NUMBER())
				 END
			  ELSE
				THROW
			 IF @RetryCount >@MaxRetryCount 
				THROW
		END CATCH 
	END
END;
 
/****** Object:  StoredProcedure [audit].[usp_PipelineStart]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Start a Pipeline AND return all Meta Data (complex)
Used By:		ADF Pipeline-Worker 

-- Example
	exec [audit].[usp_PipelineStart]  37, '7EEFA88E-D9F9-4204-AC47-97F2FC3362CE', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82', '1087FDF2-90FB-437B-A3CB-D2F94E8C4D82'
	SELECT * FROM audit.PipelineLog ORDER BY StartDateTime DESC 
	SELECT * FROM audit.LineageLog ORDER BY LineageKey DESC 
	exec [audit].[usp_PipelineStart] 37,  'cfb66af5-4815-4664-a494-f08fdcf9e0ab'
History:	
	24/11/2024 Bob, Migrated to Fabric Databaase

*/
CREATE  PROC [audit].[usp_PipelineStart]  
	@PipelineID [int] 
	,@LineageKey uniqueidentifier = null 
	,@RunID uniqueidentifier = null 
	,@ParentRunID uniqueidentifier = null
	,@WorkspaceID uniqueidentifier = null
	,@Pipeline uniqueidentifier = null
	,@PipelineName varchar(512) = null
	,@PackageGroup varchar(512) =null
AS
BEGIN

	DECLARE  @SourceConnectionSettings	varchar(8000)
			,@TargetConnectionSettings	varchar(8000)
			,@SourceSettings			varchar(8000)
			,@TargetSettings				varchar(8000)
			,@ActivitySettings			varchar(8000)
			,@PreExecuteSQL				varchar(8000)
			,@PostExecuteSQL			varchar(8000)
			,@Template				varchar(8000)
			,@StartDateTime				datetime2 = getdate()
			,@Stage						varchar(50)
			,@PipelineSequence			int
			,@MonitoringUrl				varchar(512)
			,@RetryCount				int = 0
	        ,@MaxRetryCount			    int = 3
			,@Success					BIT = 0
			,@TableID					int = null


	SET @LineageKey=COALESCE(@LineageKey,  NEWID())
	SELECT @SourceConnectionSettings=SourceConnectionSettings
	, @TargetConnectionSettings=TargetConnectionSettings
	, @SourceSettings=SourceSettings
	, @TargetSettings=TargetSettings
	, @ActivitySettings=ActivitySettings
	, @PreExecuteSQL=PreExecuteSQL
	, @PostExecuteSQL=PostExecuteSQL
	, @Template=Template
	, @Stage=Stage
	,@MonitoringUrl = lower('https://app.powerbi.com/workloads/data-pipeline/monitoring/workspaces/' + CONVERT(varchar(36), @WorkspaceID) + '/pipelines/' + @PipelineName + '/' + CONVERT(varchar(36), @RunID) )
	,@TableID = TableID
	FROM [config].[PipelineMeta]
	WHERE PipelineID=@PipelineID
	WHILE @Success = 0
	BEGIN
		BEGIN TRY 			
			INSERT INTO [audit].[PipelineLog]	(
				 [LineageKey]
				, [RunID]
				, [ParentRunID]
				, [PipelineID]
				, WorkspaceID
				, [PackageGroup]
				, [Stage]
				, [StartDate]
				, [StartDateTime]
				, [Status]
				, PipelineSequence
				, [Template]
				, Pipeline
				, PipelineName
				, MonitoringUrl
				)

			VALUES (@LineageKey
				  ,@RunID 
				  ,@ParentRunID 
				  ,@PipelineID 
				  ,@WorkspaceID
				  ,@PackageGroup
				  ,@Stage 
				  ,CONVERT(DATE, @StartDateTime)
				  ,@StartDateTime
				  ,'Started'
				  ,@PipelineSequence 
				  ,@Template 
				  ,@Pipeline
				  ,@PipelineName
				  ,@MonitoringUrl)
			 SET @Success = 1
			END TRY
			BEGIN CATCH
			 IF ERROR_NUMBER() IN (24556) /* Update Concurrency Error */
				BEGIN
					SET @RetryCount = @RetryCount + 1  
					DECLARE @BackoffDelay varchar(12) = RIGHT('00:00:' + CAST(LEAST(30.0, 0.1 * POWER(5, @RetryCount - 1)) AS VARCHAR(10)) + '00', 12)
					WAITFOR DELAY  @BackoffDelay
				 END
			  ELSE
				THROW
			 IF @RetryCount >@MaxRetryCount 
				THROW
		END CATCH 
	END
	IF @PreExecuteSQL IS NOT NULL 
	BEGIN
		PRINT ('/*Executing Pre Execute*/' + char(13) + @PreExecuteSQL)
		EXEC( @PreExecuteSQL )
	END	

	SELECT @RunID AS RunID
		  ,@PipelineID AS PipelineID
		  ,@SourceConnectionSettings AS	SourceConnectionSettings
		  ,@TargetConnectionSettings AS	TargetConnectionSettings
		  ,@SourceSettings AS SourceSettings
		  ,@TargetSettings AS TargetSettings
		  ,@ActivitySettings AS	ActivitySettings
		  ,@PreExecuteSQL AS PreExecuteSQL
		  ,@PostExecuteSQL AS PostExecuteSQL
		  ,@Template AS	Template
		  ,@LineageKey AS LineageKey
		  ,@TableID AS TableID
END;
 
/****** Object:  StoredProcedure [config].[usp_GetTables]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
	Return Config Tables in Dependancy Order
	15-01-2025 Shruti, Added filter to return tables with config schema in recursive CTE
*/
CREATE PROCEDURE [config].[usp_GetTables]
AS
BEGIN
    

	;WITH cte(lvl, object_id, name) AS (SELECT        1 AS Expr1, object_id, name
    FROM            sys.tables
    WHERE        (type_desc = 'USER_TABLE') AND (is_ms_shipped = 0) and name <> 'sysdiagrams' and SCHEMA_NAME(schema_id) = 'config'
    UNION ALL
    SELECT        cte_2.lvl + 1 AS Expr1, t.object_id, t.name
    FROM            cte AS cte_2 INNER JOIN
                            sys.tables AS t ON EXISTS
                                (SELECT        NULL AS Expr1
                                    FROM            sys.foreign_keys AS fk
                                    WHERE        (parent_object_id = t.object_id) AND (referenced_object_id = cte_2.object_id)) AND t.object_id <> cte_2.object_id AND cte_2.lvl < 30
    WHERE        (t.type_desc = 'USER_TABLE') AND (t.is_ms_shipped = 0) and SCHEMA_NAME(schema_id) = 'config')
    SELECT        TOP (100) PERCENT name, MAX(lvl) AS dependency_level
     FROM            cte AS cte_1
     GROUP BY name
     ORDER BY dependency_level, name

END;
 
/****** Object:  StoredProcedure [config].[usp_OpsDatasets]    Script Date: 07/03/2025 15:58:59 ******/
CREATE     PROCEDURE [config].[usp_OpsDatasets]
AS
BEGIN
    

	SELECT Dataset, Monitored, MaxAgeHours, PackageGroup, workspace
	FROM config.Datasets ds
	INNER JOIN Configurations c on c.ConfigurationID =ds.ConfigurationID
	CROSS APPLY OPENJSON (c.ConnectionSettings) WITH (
		workspace varchar(128)
	) j
	WHERE c.[Enabled]=1 
END;
 
/****** Object:  StoredProcedure [config].[usp_PipelineBuildMetaData]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description: Build PipelineMeta table 
Example:		
	exec [config].[usp_PipelineBuildMetaData]
History:
	11/11/2024	Kristan, removed ConfigurationValue
	11/11/2024	Kristan, fixed p and pg connections settings
	03/12/2024 	Kristan, Removed TemplateType from config.Templates
*/

CREATE  PROC [config].[usp_PipelineBuildMetaData]
@PipelineID int =null
AS
BEGIN
		
	DECLARE @Finished		bit=0
			,@OldPipelineID INT=0
			,@SinglePipeline bit=0

	IF @PipelineID is not null
		BEGIN
			DELETE FROM [config].[PipelineMeta] WHERE PipelineID=@PipelineID
			SET @SinglePipeline=1
		END
	ELSE
		DELETE FROM [config].[PipelineMeta]

	IF coalesce(@PipelineID,0) =0 
		SELECT TOP 1 @PipelineID =PipelineID 
		FROM Pipelines 
		WHERE PipelineID > @OldPipelineID
	ORDER BY PipelineID

	SET @OldPipelineID =@PipelineID

	WHILE @Finished =0
	BEGIN
		DECLARE @SourceConnectionSettings VARCHAR(8000) = NULL
				,@SourceConnectionSettings1 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings2 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings3 VARCHAR(8000) = NULL
			   ,@SourceConnectionSettings4 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings1 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings2 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings3 VARCHAR(8000) = NULL
			   ,@TargetConnectionSettings4 VARCHAR(8000) = NULL
			   ,@SourceSettings1 VARCHAR(8000) = NULL
			   ,@SourceSettings2 VARCHAR(8000) = NULL
			   ,@SourceSettings3 VARCHAR(8000) = NULL
			   ,@SourceSettings VARCHAR(8000) = NULL
			   ,@TargetSettings1 VARCHAR(8000) = NULL
			   ,@TargetSettings2 VARCHAR(8000) = NULL
			   ,@TargetSettings3 VARCHAR(8000) = NULL
			   ,@TargetSettings VARCHAR(8000) = NULL
			   ,@Template VARCHAR(255) = NULL
			   ,@ActivitySettings VARCHAR(8000) = NULL
			   ,@ActivitySettings1 VARCHAR(8000) = NULL
			   ,@ActivitySettings2 VARCHAR(8000) = NULL
			   ,@PackageGroup VARCHAR(50) = NULL
			   ,@PreExecuteSQL VARCHAR(8000) = NULL
			   ,@PostExecuteSQL VARCHAR(8000) = NULL
			   ,@Enabled INT = NULL
			   ,@Stage VARCHAR(20) = NULL
			   ,@PipelineSequence SMALLINT = NULL
			   ,@SourceObject VARCHAR(512) = NULL
			   ,@TargetSchemaName VARCHAR(128) = NULL
			   ,@TargetTableName VARCHAR(128) = NULL
			   ,@TargetDirectory VARCHAR(512) = NULL
			   ,@TargetFileName VARCHAR(128) = NULL
			   ,@StartDateTime DATETIME2(6) = NULL
			   ,@ActivityType VARCHAR(10) = NULL
			   ,@Pipeline varchar(200) = NULL
			   ,@TemplateType varchar(50) = NULL
			   ,@TableID int = NULL
			   ,@ID UNIQUEIDENTIFIER = NULL

		SELECT	@Pipeline=p.Pipeline
			  ,@Template = COALESCE(p.Template, pg.Template)
			  ,@TargetConnectionSettings1 = p.TargetConnectionSettings
			  ,@TargetConnectionSettings2 = pg.TargetConnectionSettings
			  ,@TargetConnectionSettings3 = c2.ConnectionSettings
			  ,@TargetConnectionSettings4 = c4.ConnectionSettings
			  ,@SourceConnectionSettings1 = p.SourceConnectionSettings
			  ,@SourceConnectionSettings2 = pg.SourceConnectionSettings
			  ,@SourceConnectionSettings3 = c1.ConnectionSettings
			  ,@SourceConnectionSettings4 = c3.ConnectionSettings
			  ,@SourceSettings1 = nullif(p.SourceSettings,'')
			  ,@SourceSettings2 = nullif(pg.SourceSettings,'')
			  ,@SourceSettings3 = nullif(t.SourceSettings,'')
			  ,@TargetSettings1 = nullif(p.TargetSettings,'')
			  ,@TargetSettings2 = nullif(pg.TargetSettings,'')
			  ,@TargetSettings3 = nullif(t.TargetSettings,'')
			  ,@ActivitySettings1 = nullif(p.ActivitySettings,'')
			  ,@ActivitySettings2 = nullif(pg.ActivitySettings,'')
			  ,@PipelineSequence = p.PipelineSequence
			  ,@PackageGroup = COALESCE(p.PackageGroup, pg.PackageGroup)
			  ,@PreExecuteSQL = COALESCE(p.PreExecuteSQL, pg.PreExecuteSQL)
			  ,@PostExecuteSQL = COALESCE(p.PostExecuteSQL, pg.PostExecuteSQL)
			  ,@Enabled = CASE WHEN p.Enabled = 0 OR pg.Enabled = 0 THEN 0 ELSE 1 END
			  ,@Stage = COALESCE(pg.Stage, p.Stage)
			  ,@StartDateTime = GETDATE()
			  ,@ActivityType = CASE WHEN LEFT(LTRIM(COALESCE(p.ActivitySettings, pg.ActivitySettings)),1) = '{' THEN 'JSON' WHEN LEFT(LTRIM(COALESCE(p.ActivitySettings, pg.ActivitySettings)),1) = '<' THEN 'XML' ELSE 'OTHER' END
			  ,@TemplateType= t.ArtefactType
			  ,@ID=a.artefact_id
			  ,@TableID = p.TableID
			  FROM [Pipelines] p
		LEFT JOIN config.PipelineGroups pg ON pg.PipelineGroupID = p.PipelineGroupID
		LEFT JOIN config.[Templates] t on coalesce(p.[Template],pg.[Template]) =t.[Template]
		LEFT JOIN [config].[Configurations] c1 on pg.SourceConfigurationID = c1.ConfigurationID
		LEFT JOIN [config].[Configurations] c2 on pg.TargetConfigurationID = c2.ConfigurationID
		LEFT JOIN [config].[Configurations] c3 on p.SourceConfigurationID = c3.ConfigurationID
		LEFT JOIN [config].[Configurations] c4 on p.TargetConfigurationID = c4.ConfigurationID
		LEFT JOIN config.Artefacts a on t.ArtefactType in ('Notebook','DataPipeline') and t.Template = a.artefact
		WHERE p.PipelineID = @PipelineID

		--DEBUG
		--SELECT @Pipeline AS Pipeline,@Template AS Template,@SourceConnectionSettings1 AS SourceConnectionSettings1,@SourceConnectionSettings2 AS SourceConnectionSettings2,@SourceConnectionSettings3 AS SourceConnectionSettings3,@SourceConnectionSettings4 AS SourceConnectionSettings4,@TargetConnectionSettings1 AS TargetConnectionSettings1,@TargetConnectionSettings2 AS TargetConnectionSettings2,@TargetConnectionSettings3 AS TargetConnectionSettings3,@TargetConnectionSettings4 AS TargetConnectionSettings4,@SourceSettings1 AS SourceSettings1,@SourceSettings2 AS SourceSettings2,@SourceSettings3 AS SourceSettings3,@TargetSettings1 AS TargetSettings1,@TargetSettings2 AS TargetSettings2,@TargetSettings3 AS TargetSettings3,@ActivitySettings1 AS ActivitySettings1,@ActivitySettings2 AS ActivitySettings2,@PipelineSequence AS PipelineSequence,@PackageGroup AS PackageGroup,@PreExecuteSQL AS PreExecuteSQL,@PostExecuteSQL AS PostExecuteSQL,@Enabled AS Enabled,@Stage AS Stage,@StartDateTime AS StartDateTime,@ActivityType AS ActivityType,@TemplateType AS TemplateType,@ID AS ArtefactID, @TableID as TableID
		
		PRINT  @PipelineID

		IF @ActivityType = 'JSON'
			BEGIN 
				;WITH AcS1 AS (SELECT * FROM OPENJSON(@ActivitySettings1)),
					  AcS2 AS (SELECT * FROM OPENJSON(@ActivitySettings2))
				 SELECT @ActivitySettings = (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM 
											(
												SELECT * FROM AcS1
												UNION ALL
												SELECT * FROM AcS2
												WHERE [key] NOT IN (SELECT [key] FROM AcS1)
											) s)
			END
		ELSE 
			SET @ActivitySettings = COALESCE(@ActivitySettings1, @ActivitySettings2)
		
		;WITH SS1 AS (SELECT * FROM OPENJSON(@SourceSettings1)),
			  SS2 AS (SELECT * FROM OPENJSON(@SourceSettings2)),
			  SCS1 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings1)),
			  SCS2 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings2)),
			  SCS3 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings3)),
			  SCS4 AS (SELECT * FROM OPENJSON(@SourceConnectionSettings4)),
			  TS1 AS (SELECT * FROM OPENJSON(@TargetSettings1)),
			  TS2 AS (SELECT * FROM OPENJSON(@TargetSettings2)),
			  TCS1 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings1)),
			  TCS2 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings2)),
			  TCS3 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings3)),
			  TCS4 AS (SELECT * FROM OPENJSON(@TargetConnectionSettings4))
		SELECT	@SourceConnectionSettings =  (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM (
                    SELECT * FROM SCS1
					UNION ALL
					SELECT * FROM SCS4
					WHERE EXISTS (SELECT 1 FROM SCS4)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)

					-- If SCS4 does not exist, follow the SCS3, SCS2, SCS1 priority
					UNION ALL
					SELECT * FROM SCS3
					WHERE NOT EXISTS (SELECT 1 FROM SCS4)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)
          
					UNION ALL
					SELECT * FROM SCS2
					WHERE NOT EXISTS (SELECT 1 FROM SCS4)
					  AND NOT EXISTS (SELECT 1 FROM SCS3)
					  AND [key] NOT IN (SELECT [key] FROM SCS1)
				) s),
				@SourceSettings =  (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM 
	 								(
	 										SELECT * FROM SS1
	 										UNION ALL
	 										SELECT * FROM SS2
	 										WHERE [key] NOT IN (SELECT [key] FROM SS1)
	 										UNION ALL
	 										SELECT * FROM OPENJSON(@SourceSettings3)
	 										WHERE [key] NOT IN (SELECT [key] FROM SS1)
	 										  AND [key] NOT IN (SELECT [key] FROM SS2)
	 								) s ),
				@TargetConnectionSettings =  (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM 
	 								(
	 										SELECT * FROM TCS1
											UNION ALL
											SELECT * FROM TCS4
											WHERE EXISTS (SELECT 1 FROM TCS4)
												AND [key] NOT IN (SELECT [key] FROM TCS1)

											UNION ALL
											SELECT * FROM TCS3
											WHERE NOT EXISTS (SELECT 1 FROM TCS4)
												AND [key] NOT IN (SELECT [key] FROM TCS1)

											UNION ALL
											SELECT * FROM TCS2
											WHERE NOT EXISTS (SELECT 1 FROM TCS4)
												AND NOT EXISTS (SELECT 1 FROM TCS3)
												AND [key] NOT IN (SELECT [key] FROM TCS1)
											) s),
				@TargetSettings = (SELECT '{' + STRING_AGG('"' + s.[key] + '":' + CASE WHEN s.type = 1 THEN '"' + s.[value] + '"' WHEN s.type = 0 THEN 'null' ELSE s.[value] END,',') + '}' FROM 
									(
											SELECT * FROM TS1
											UNION ALL
											SELECT * FROM TS2
											WHERE [key] NOT IN (SELECT [key] FROM TS1)
											UNION ALL 
											SELECT * FROM OPENJSON(@TargetSettings3)
											WHERE [key] NOT IN (SELECT [key] FROM TS1)
											  AND [key] NOT IN (SELECT [key] FROM TS2)
									) s)
			
		IF NOT EXISTS (SELECT * FROM [config].[PipelineMeta] WHERE PipelineID=@PipelineID)
			INSERT INTO [config].[PipelineMeta]	([PipelineID], [Pipeline], [Template], [SourceConnectionSettings], [TargetConnectionSettings], [SourceSettings], [TargetSettings], [ActivitySettings], [PipelineSequence], [PackageGroup], [PreExecuteSQL], [PostExecuteSQL], [Enabled], [Stage],TemplateType, TemplateID,TableID)
			VALUES (@PipelineID, @Pipeline,@Template, @SourceConnectionSettings, @TargetConnectionSettings, @SourceSettings, @TargetSettings, @ActivitySettings, @PipelineSequence, @PackageGroup, @PreExecuteSQL, @PostExecuteSQL, @Enabled, @Stage,@TemplateType,@ID, @TableID)
	
		SELECT TOP 1 @PipelineID =PipelineID 
		FROM Pipelines 
		WHERE PipelineID > @OldPipelineID
		ORDER BY PipelineID

		IF @PipelineID is null OR @PipelineID=@OldPipelineID OR @SinglePipeline=1
			SET @Finished=1
		ELSE
			SET @OldPipelineID =@PipelineID
	END
END;
 
/****** Object:  StoredProcedure [config].[usp_PipelineQueue]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Return a Queue of Pipelines to Run based on PipelineSequence
Used By:		ADF Pipeline-Worker 

Example
	exec config.[usp_PipelineQueue] 'AW'

History:	
	30/04/2024 Bob, Migrated to Fabric
	27/08/2024 Aidan, Implemented package group hierarchy
	29/08/2024 Bob, Added TemplateType for Dynamic Pipeline-Worker
	11/09/2024 Aidan, Ignores PackageGroup if PipelineID is specified
	31/09/2024 Kristan, added to config schema
	06/11/2024 Aidan, Added TableID 
*/
CREATE    PROC [config].[usp_PipelineQueue] 
	@PackageGroup [varchar](50) ='ALL'
	,@PipelineSequence [smallint] =-1
	,@PipelineID [int] =-1
AS
BEGIN
	
	DECLARE @ChecksumToken INT
	DECLARE @OldChecksumToken INT

	IF @PipelineID <> -1 AND @PipelineID IS NOT NULL
	BEGIN
		SELECT @PackageGroup='ALL',@PipelineSequence=-1
	END

	SELECT @PackageGroup=COALESCE(@PackageGroup, 'ALL'), @PipelineSequence=COALESCE(@PipelineSequence,-1)


	SELECT @OldChecksumToken=ChecksumToken FROM [config].[PipelineMetaCache]
	SELECT @ChecksumToken = CHECKSUM_AGG(checksum(*)) 
	FROM config.Pipelines p
	INNER JOIN config.PipelineGroups pg ON pg.PipelineGroupID =p.PipelineGroupID
	LEFT JOIN config.Configurations c on pg.TargetConfigurationID =c.ConfigurationID
	LEFT JOIN config.Configurations c2 on pg.SourceConfigurationID =c2.ConfigurationID
		
	IF @OldChecksumToken <> @ChecksumToken or @OldChecksumToken is null
	BEGIN
		PRINT 'Rebuilding Cache. exec [config].[usp_PipelineBuildMetaData]'
		exec [config].[usp_PipelineBuildMetaData]

		IF EXISTS (SELECT * FROM [config].[PipelineMetaCache] )
			UPDATE [config].[PipelineMetaCache] SET ChecksumToken=@ChecksumToken
		ELSE
			INSERT INTO [config].[PipelineMetaCache] VALUES (@ChecksumToken)	
	END


	;WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup FROM string_split (@PackageGroup,',') WHERE value <> 'ALL'
		UNION ALL
        SELECT PackageGroup FROM config.PackageGroups pg WHERE  @PackageGroup='ALL'
		UNION 
		SELECT pgl.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value) 
		UNION 
		SELECT pgl2.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl  ON pgl.PackageGroup  = TRIM(pg2.value) 
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup =  pgl.ChildPackageGroup
	)
	SELECT NEWID() as LineageKey, [PipelineID], [PipelineSequence],[SourceConnectionSettings],[TargetConnectionSettings],[SourceSettings],[TargetSettings],[ActivitySettings],[PreExecuteSQL],[PostExecuteSQL],[Stage]
	, [Template], coalesce(TemplateType, Template) as TemplateType, TemplateID, p.TableID as TableID
	FROM [config].[PipelineMeta] p
	WHERE p.Enabled=1 
	AND (p.PackageGroup=@PackageGroup OR p.[PackageGroup] IN (SELECT PackageGroup FROM pg) or @PackageGroup ='ALL')
	AND (p.PipelineID=@PipelineID OR @PipelineID=-1)
	AND (p.PipelineSequence=@PipelineSequence OR @PipelineSequence =-1) 
	ORDER BY p.PipelineSequence, p.PipelineID 


END;
 
/****** Object:  StoredProcedure [config].[usp_PipelineSequence]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Retrieve Array of PipelineOrder for Which Pipelines to run in which sequence
Used By:		ADF Pipeline-Controller 

-- Example
	exec [config].[usp_PipelineSequence] 'AW','ALL'
	exec [config].[usp_PipelineSequence] 'test-copy-blob'
	exec [config].[usp_PipelineSequence] 'GAMDAY0','Ingest'
	   	 
History:	
	08/05/2024 Bob, Migrated to Fabric 
	27/08/2024 Aidan, Implemented package group hierarchy
	29/08/2024 Bob, Trigger build of meta data cache if needed
	01/11/2024 Kristan, Added PackageGroup coalesce for PipelineGroups
	11/11/2024 Aiddan, Changed join to Pipelinegroups to a Left join

*/
CREATE   PROC [config].[usp_PipelineSequence] 
	@PackageGroup [varchar](4000) ='ALL'
	,@Stage [varchar](50) =null
AS
BEGIN
	
	DECLARE @ChecksumToken INT
	DECLARE @OldChecksumToken INT

	SELECT @OldChecksumToken=ChecksumToken FROM [config].[PipelineMetaCache]
	SELECT @ChecksumToken = CHECKSUM_AGG(checksum(*)) 
	FROM config.Pipelines p
	LEFT JOIN config.PipelineGroups pg ON pg.PipelineGroupID =p.PipelineGroupID
	LEFT JOIN config.Configurations c on pg.TargetConfigurationID =c.ConfigurationID
	LEFT JOIN config.Configurations c2 on pg.SourceConfigurationID =c2.ConfigurationID
		
	IF @OldChecksumToken <> @ChecksumToken or @OldChecksumToken is null
	BEGIN
		PRINT 'Rebuilding Cache. exec [config].[usp_PipelineBuildMetaData]'
		exec [config].[usp_PipelineBuildMetaData]

		IF EXISTS (SELECT * FROM [config].[PipelineMetaCache] )
			UPDATE [config].[PipelineMetaCache] SET ChecksumToken=@ChecksumToken
		ELSE
			INSERT INTO [config].[PipelineMetaCache] VALUES (@ChecksumToken)	
	END

	SELECT @PackageGroup =coalesce(@PackageGroup,'ALL')
	,@Stage =coalesce(NULLIF(@Stage,''),'ALL')
	
	;WITH pg as 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup FROM string_split (@PackageGroup,',') WHERE value <> 'ALL'
		UNION ALL
        SELECT PackageGroup FROM config.PackageGroups pg WHERE @PackageGroup='ALL'
		UNION 
		SELECT pgl.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value) 
		UNION 
		SELECT pgl2.ChildPackageGroup AS PackageGroup FROM string_split(@PackageGroup,',') pg2 
		INNER JOIN config.PackageGroupLinks pgl  ON pgl.PackageGroup  = TRIM(pg2.value) 
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup =  pgl.ChildPackageGroup
	),
	s as 
	(
		SELECT value as Stage FROM string_split (@Stage,',') WHERE value <> 'ALL'
	)
	SELECT p.[PipelineSequence],max(coalesce(p.Stage, pg.Stage)) as Stage
	, convert(bit,max(coalesce(p.ContinueOnError, pg.ContinueOnError, 0 ))) as ContinueOnError
	FROM [config].[Pipelines] p
	LEFT JOIN config.PipelineGroups pg on pg.PipelineGroupID =p.PipelineGroupID
	WHERE coalesce(p.[PackageGroup],pg.[PackageGroup]) IN (SELECT PackageGroup FROM pg) 
	AND (coalesce(p.Stage,pg.Stage )  IN (SELECT Stage FROM s) OR @Stage ='ALL')
	GROUP BY p.PipelineSequence
	ORDER BY p.PipelineSequence

END;
 
/****** Object:  StoredProcedure [config].[usp_TruncateAll]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
	Truncate ALL Config Tables. 
	Only run this for deployment or recovery
	15-01-2025 Shruti, Updated Query to return Schema Name within CTE  
*/
CREATE PROCEDURE [config].[usp_TruncateAll]
AS
BEGIN
    
	DECLARE @sql nvarchar(max)

	-- DIASBLE Triggers
	DROP TABLE IF EXISTS #tr
	SELECT t.Name AS TriggerName, o.Name AS TableName 
	INTO #tr
	FROM sys.triggers t 
	INNER JOIN sys.objects o ON t.parent_id = o.object_id
	INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
	WHERE s.name = 'config'  AND t.type = 'TR'
	SELECT @sql = STRING_AGG ('DISABLE TRIGGER [' + TriggerName + '] ON [config].[' + Tablename + ']', ';')
	FROM #tr 
	print @sql

	exec (@sql)

	--DELETE Data from tables
	;WITH cte(lvl, object_id, name, SchemaName) AS (SELECT        1 AS Expr1, object_id, name, SCHEMA_NAME(schema_id) AS SchemaName
    FROM            sys.tables
    WHERE        (type_desc = 'USER_TABLE') AND (is_ms_shipped = 0) and name <> 'sysdiagrams' and SCHEMA_NAME(schema_id)='config'
    UNION ALL
    SELECT        cte_2.lvl + 1 AS Expr1, t.object_id, t.name, SCHEMA_NAME(schema_id) AS SchemaName
    FROM            cte AS cte_2 INNER JOIN
                            sys.tables AS t ON EXISTS
                                (SELECT        NULL AS Expr1
                                    FROM            sys.foreign_keys AS fk
                                    WHERE        (parent_object_id = t.object_id) AND (referenced_object_id = cte_2.object_id)) AND t.object_id <> cte_2.object_id AND cte_2.lvl < 30
    WHERE        (t.type_desc = 'USER_TABLE') AND (t.is_ms_shipped = 0) )
	, t as (
	    SELECT        TOP (100) PERCENT SchemaName, name, MAX(lvl) AS dependency_level
		FROM            cte AS cte_1
		GROUP BY SchemaName, name
		ORDER BY dependency_level, name
	 )
	 SELECT @sql = STRING_AGG  ('DELETE FROM ' + SchemaName + '.' + name,';')  WITHIN GROUP (ORDER BY dependency_level DESC, name DESC)
	FROM t
  	print @sql

	exec (@sql)

	--ENABLE Triggers after deletion is completed
	SELECT @sql = STRING_AGG ('ENABLE TRIGGER [' + TriggerName + '] ON [config].[' + Tablename + ']', ';')
	FROM #tr 
	print @sql

	exec (@sql)

END;
 
/****** Object:  StoredProcedure [devops].[usp_ConfigPackageTables]    Script Date: 07/03/2025 15:58:59 ******/
 
/*
Description:	Return Templates, config.PipelineGroups, config.Pipelines tables of associated PackageGroup or PipelineID 
Used By:		Deploy-Artefacts

Example
	exec [devops].[usp_ConfigPackageTables] 'AW'

History:	
	06/01/2024 Aidan, Created Procedure

*/
CREATE     PROCEDURE [devops].[usp_ConfigPackageTables]
	@PackageGroup VARCHAR(8000) = NULL,
	@PipelineID INT = NULL
AS 
BEGIN
	IF @PipelineID IS  NULL
	BEGIN
		SELECT @PipelineID=-1
	END
	IF @PipelineID <> -1 AND @PipelineID IS NOT NULL
	BEGIN
		SELECT @PackageGroup='ALL'
	END;
	SELECT @PackageGroup = coalesce(@PackageGroup,'ALL');
	IF @PackageGroup = 'ALL' and @PipelineID <> -1
	BEGIN
		DECLARE @InitialPackageGroup NVARCHAR(255);
		SET @InitialPackageGroup = @PackageGroup;

		SELECT @PackageGroup = COALESCE(p.PackageGroup, pg.PackageGroup, @InitialPackageGroup)
		FROM config.Pipelines p
		LEFT JOIN config.PipelineGroups pg 
			ON p.PipelineGroupID = pg.PipelineGroupID
		WHERE p.PipelineID = @PipelineID;
	END;
	WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	SELECT t.*
	FROM config.Templates t
	WHERE t.Template IN 
	(
		SELECT pg.Template 
		FROM config.PipelineGroups pg
		WHERE pg.PipelineGroupID IN 
		(
			SELECT p.PipelineGroupID
			FROM config.Pipelines p
			WHERE p.Enabled = 1 
			AND (p.PackageGroup = @PackageGroup 
				 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
				 OR @PackageGroup = 'ALL')
			AND (p.PipelineID = @PipelineID OR @PipelineID = -1)
		)
	) ORDER BY t.Template ASC;

	
		WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	SELECT pg.*
	FROM config.PipelineGroups pg
	WHERE pg.PipelineGroupID IN 
	(
		SELECT p.PipelineGroupID
		FROM config.Pipelines p
		WHERE p.Enabled = 1 
		AND (p.PackageGroup = @PackageGroup 
			 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
			 OR @PackageGroup = 'ALL')
		AND (p.PipelineID = @PipelineID OR @PipelineID = -1)
	) ORDER BY pg.PipelineGroupID ASC;
		WITH pg AS 
	(
		SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
		FROM string_split (@PackageGroup, ',') 
		WHERE value <> 'ALL'
		
		UNION ALL
		
		SELECT PackageGroup 
		FROM config.PackageGroups pg 
		WHERE @PackageGroup = 'ALL'
		
		UNION 
		
		SELECT pgl.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
		UNION 
		
		SELECT pgl2.ChildPackageGroup AS PackageGroup 
		FROM string_split(@PackageGroup, ',') pg2 
		INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
	)
	
	SELECT *
	FROM config.Pipelines p
	WHERE p.Enabled = 1 
	AND (p.PackageGroup = @PackageGroup 
		 OR p.PackageGroup IN (SELECT PackageGroup FROM pg)
		 OR @PackageGroup = 'ALL')
	AND (p.PipelineID = @PipelineID OR @PipelineID = '-1')
	ORDER BY p.PipelineID ASC;
	WITH RelevantPackages AS 
	(

		SELECT TRIM(value) AS PackageGroup 
		FROM string_split(@PackageGroup, ',') 
		WHERE value <> 'ALL'
    
		UNION ALL
    
		SELECT PackageGroup 
		FROM config.PackageGroups 
		WHERE @PackageGroup = 'ALL' and @PipelineID = -1
		)

		SELECT 
			pgl.PackageGroup, 
			pgl.ChildPackageGroup
		FROM 
			config.PackageGroupLinks pgl
		WHERE 
			pgl.PackageGroup IN (SELECT PackageGroup FROM RelevantPackages)
		ORDER BY 
			pgl.PackageGroup, 
			pgl.ChildPackageGroup;
		WITH pg AS 
		(
			SELECT CONVERT(VARCHAR(50), TRIM(value)) AS PackageGroup 
			FROM string_split (@PackageGroup, ',') 
			WHERE value <> 'ALL'
		
			UNION ALL
		
			SELECT PackageGroup 
			FROM config.PackageGroups pg 
			WHERE @PackageGroup = 'ALL' and @PipelineID = -1
		
			UNION 
		
			SELECT pgl.ChildPackageGroup AS PackageGroup 
			FROM string_split(@PackageGroup, ',') pg2 
			INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
		
			UNION 
		
			SELECT pgl2.ChildPackageGroup AS PackageGroup 
			FROM string_split(@PackageGroup, ',') pg2 
			INNER JOIN config.PackageGroupLinks pgl ON pgl.PackageGroup = TRIM(pg2.value)
			INNER JOIN config.PackageGroupLinks pgl2 ON pgl2.PackageGroup = pgl.ChildPackageGroup
		)
		SELECT 
			pg.PackageGroup, 
			pg.Monitored, 
			pg.SortOrder
		FROM 
			config.PackageGroups pg
		WHERE 
			pg.PackageGroup IN (SELECT PackageGroup FROM pg) -- Check if the PackageGroup matches
		ORDER BY 
			pg.PackageGroup;

END;
