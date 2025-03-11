CREATE TABLE [config].[Datasets] (
    [Dataset]         VARCHAR (50)  NOT NULL,
    [Monitored]       BIT           NULL,
    [MaxAgeHours]     INT           NULL,
    [PackageGroup]    VARCHAR (50)  NULL,
    [ConfigurationID] INT           NULL,
    [DataOwner]       VARCHAR (255) NULL,
    CONSTRAINT [PK_Datasets] PRIMARY KEY CLUSTERED ([Dataset] ASC),
    CONSTRAINT [FK_Datasets_Configurations] FOREIGN KEY ([ConfigurationID]) REFERENCES [config].[Configurations] ([ConfigurationID])
);


GO

