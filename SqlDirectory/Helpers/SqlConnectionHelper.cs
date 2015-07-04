using System.Data.SqlClient;
using Dapper;

namespace SqlDirectory
{
    internal static class SqlConnectionHelper
    {
        public static void DropTableIfExists(this SqlConnection connection, string schemaName, string tableName)
        {
            var exists = 1 == connection.ExecuteScalar<int>(@"SELECT count(0) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME=@tableName AND TABLE_Schema = @schemaName", new { schemaName = SqlHelper.RemoveBrackets(schemaName), tableName = SqlHelper.RemoveBrackets(tableName) });
            if (exists)
            {
                connection.Execute($"DROP TABLE {schemaName}.{tableName}");
            }
        }

        public static bool SchemaExists(this SqlConnection connection, string schemaName)
        {
            return 1 == connection.ExecuteScalar<int>("SELECT count(0) FROM information_schema.schemata WHERE   schema_name = @schemaName", new { schemaName = SqlHelper.RemoveBrackets(schemaName) });
        }
    }
}