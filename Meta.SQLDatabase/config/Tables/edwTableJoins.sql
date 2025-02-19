CREATE TABLE [config].[edwTableJoins] (
    [TableID]        SMALLINT      NOT NULL,
    [RelatedTableID] SMALLINT      NOT NULL,
    [ShowBK]         BIT           NULL,
    [JoinSQL]        VARCHAR (512) NULL,
    [AliasPK]        VARCHAR (50)  NULL,
    [JoinOrder]      SMALLINT      NULL,
    [JoinType]       VARCHAR (20)  NULL,
    CONSTRAINT [PK_edwTableJoins] PRIMARY KEY CLUSTERED ([TableID] ASC, [RelatedTableID] ASC),
    CONSTRAINT [FK_edwTableJoins_edwTables] FOREIGN KEY ([TableID]) REFERENCES [config].[edwTables] ([TableID]),
    CONSTRAINT [FK_edwTableJoins_edwTables1] FOREIGN KEY ([RelatedTableID]) REFERENCES [config].[edwTables] ([TableID])
);


GO

