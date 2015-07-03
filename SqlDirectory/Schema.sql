DROP TABLE [FileMetaData];
DROP TABLE [Locks]
DROP TABLE [FileContents]


CREATE TABLE dbo.FileMetaData 
( 
	Id BIGINT PRIMARY KEY IDENTITY(1,1) NOT NULL,
	Name NVARCHAR(400) NOT NULL,
	LastTouched DATETIME2 NOT NULL
)

CREATE TABLE dbo.Locks ( Name NVARCHAR(400) NOT NULL)

CREATE TABLE [dbo].[FileContents]
(
	[Name] NVARCHAR(400) PRIMARY KEY,
	[Content] varbinary(max) NULL
)
