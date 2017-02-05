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
            yield return $"CREATE TABLE {schemaName}.[FileMetaData] ( SequenceId BIGINT IDENTITY(1,1), IsDeleted BIT DEFAULT 0, [Name] NVARCHAR(400) NOT NULL,LastTouchedTimestamp DATETIME2 NOT NULL)";
            yield return $"CREATE CLUSTERED INDEX IC_FileMetaData ON {schemaName}.[FileMetaData] (SequenceId)";
            yield return $"CREATE NONCLUSTERED INDEX NIC_FileMetaData ON {schemaName}.[FileMetaData] ([Name],[IsDeleted])";
            yield return $"CREATE TABLE {schemaName}.[Locks] ( Name NVARCHAR(400) NOT NULL, LockReleaseTimestamp DATETIME2 NOT NULL)";
            yield return $"ALTER TABLE {schemaName}.[Locks] ADD CONSTRAINT PK_Locks PRIMARY KEY NONCLUSTERED ([Name] ASC)";
            yield return $"CREATE TABLE {schemaName}.[FileContents] (SequenceId BIGINT IDENTITY(1,1), IsDeleted BIT DEFAULT 0, [Name] NVARCHAR(400) NOT NULL,[Content] varbinary(max) NULL)";
            yield return $"CREATE CLUSTERED INDEX IC_FileContents ON {schemaName}.[FileContents] (SequenceId)";
            yield return $"CREATE NONCLUSTERED INDEX NIC_Contents ON {schemaName}.[FileContents] ([Name],[IsDeleted])";
        }
    }
}