using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace LuceneNetSqlDirectory
{
    internal class SqlServerLockFactory : LockFactory
    {
        private readonly SqlConnection _connection;
        private readonly Options _options;

        internal SqlServerLockFactory(SqlConnection connection, Options options)
        {
            _connection = connection;
            _options = options;
        }

        public override Lock MakeLock(string lockName)
        {
            return new SqlServerLock(_connection, lockName, _options);
        }

        public override void ClearLock(string lockName)
        {
            _connection.Execute($"DELETE FROM {_options.SchemaName}.Locks Where Name = @name", new { name = lockName });
        }
    }
}