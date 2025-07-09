CREATE TABLE [config].[IdentityMethods] (
    [IdentityMethod] VARCHAR (20)   NOT NULL,
    [Expression]     VARCHAR (8000) NULL,
    [Description]    VARCHAR (8000) NULL,
    CONSTRAINT [PK_IdentityMethods] PRIMARY KEY CLUSTERED ([IdentityMethod] ASC)
);


GO

