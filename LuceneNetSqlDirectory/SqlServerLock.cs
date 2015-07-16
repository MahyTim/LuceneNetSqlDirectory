using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace LuceneNetSqlDirectory
{
    internal class SqlServerLock : Lock
    {
        private readonly SqlConnection _connection;
        private readonly string _lockName;
        private readonly Options _options;

        public SqlServerLock(SqlConnection connection, string lockName, Options options)
        {
            _connection = connection;
            _lockName = lockName;
            _options = options;
        }

        public override bool Obtain()
        {
            ReleaseLocksByReleaseTimestamp();
            if (IsLocked())
                return false;
            try
            {
                _connection.Execute($"INSERT INTO {_options.SchemaName}.Locks (Name,LockReleaseTimestamp) VALUES (@name,DATEADD(MINUTE, @minutesToAdd, SYSUTCDATETIME()))", new { name = _lockName, minutesToAdd = _options.LockTimeoutInMinutes });
            }
            catch (SqlException ex) when (ex.Number == 2627) // Duplicate key --> duplicate lock
            {
                return false;
            }
            return true;
        }

        private void ReleaseLocksByReleaseTimestamp()
        {
            _connection.Execute($"DELETE FROM {_options.SchemaName}.[Locks] WHERE LockReleaseTimestamp < SYSUTCDATETIME()");
        }

        public override void Release()
        {
            _connection.Execute($"DELETE FROM {_options.SchemaName}.[Locks] WHERE Name = @name", new { name = _lockName });
            ReleaseLocksByReleaseTimestamp();
        }

        public override bool IsLocked()
        {
            return _connection.ExecuteScalar<int>($"SELECT COUNT(1) FROM {_options.SchemaName}.[Locks] WHERE Name = @name AND LockReleaseTimestamp > SYSUTCDATETIME()", new { name = _lockName }) != 0;
        }
    }
}