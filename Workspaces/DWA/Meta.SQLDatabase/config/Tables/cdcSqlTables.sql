CREATE TABLE [config].[cdcSqlTables] (
    [ConnectionID]    CHAR (36)     NOT NULL,
    [LakehouseName]   VARCHAR (128) NULL,
    [Database]        VARCHAR (128) NOT NULL,
    [Table]           VARCHAR (512) NOT NULL,
    [PrimaryKeys]     VARCHAR (500) NULL,
    [max_lsn]         BINARY (10)   NULL,
    [max_datetime]    DATETIME2 (7) NULL,
    [target_lsn]      BINARY (10)   NULL,
    [target_datetime] DATETIME2 (7) NULL,
    [Enabled]         BIT           CONSTRAINT [DF_cdcSqlTables_Enabled] DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_cdcSqlTables] PRIMARY KEY CLUSTERED ([ConnectionID] ASC, [Database] ASC, [Table] ASC)
);


GO

