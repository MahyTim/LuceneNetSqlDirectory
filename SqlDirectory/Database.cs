using System.Collections.Generic;

namespace SqlDirectory
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
            yield return $"CREATE TABLE {schemaName}.[FileMetaData] ( [Name] NVARCHAR(400) NOT NULL PRIMARY KEY,LastTouched DATETIME2 NOT NULL)";
            yield return $"CREATE TABLE {schemaName}.[Locks] ( Name NVARCHAR(400) NOT NULL PRIMARY KEY, LockReleaseTimestamp DATETIME2 NOT NULL)";
            yield return $"CREATE TABLE {schemaName}.[FileContents] ([Name] NVARCHAR(400) NOT NULL PRIMARY KEY,[Content] varbinary(max) NULL)";
        }
    }
}