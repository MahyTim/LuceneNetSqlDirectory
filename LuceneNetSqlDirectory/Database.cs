using System.Collections.Generic;

namespace LuceneNetSqlDirectory
{
    public class Database
    {
        public static IEnumerable<string> Tables
        {
            get
            {
                yield return "FileMetaData";
                yield return "Locks";
                yield return "FileContents";
            }
        }

        public static IEnumerable<string> Structure(string schemaName)
        {
            yield return $"CREATE TABLE {schemaName}.[FileMetaData] ( [Name] NVARCHAR(400) NOT NULL,LastTouchedTimestamp DATETIME2 NOT NULL)";
            yield return $"ALTER TABLE {schemaName}.[FileMetaData] ADD CONSTRAINT PK_FileMetaData PRIMARY KEY NONCLUSTERED ([Name] ASC)";
            yield return $"CREATE TABLE {schemaName}.[Locks] ( Name NVARCHAR(400) NOT NULL, LockReleaseTimestamp DATETIME2 NOT NULL)";
            yield return $"ALTER TABLE {schemaName}.[Locks] ADD CONSTRAINT PK_Locks PRIMARY KEY NONCLUSTERED ([Name] ASC)";
            yield return $"CREATE TABLE {schemaName}.[FileContents] ([Name] NVARCHAR(400) NOT NULL,[Content] varbinary(max) NULL)";
            yield return $"ALTER TABLE {schemaName}.[FileContents] ADD CONSTRAINT PK_FileContents PRIMARY KEY NONCLUSTERED ([Name] ASC)";
        }
    }
}