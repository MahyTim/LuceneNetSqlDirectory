using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
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
            if (IsLocked())
                return false;
            _connection.Execute($"INSERT INTO {_options.SchemaName}.Locks (Name) VALUES (@name)", new { name = _lockName });
            return true;
        }

        public override void Release()
        {
            _connection.Execute($"DELETE FROM {_options.SchemaName}.Locks Where Name = @name", new { name = _lockName });
        }

        public override bool IsLocked()
        {
            return _connection.ExecuteScalar<int>($"SELECT COUNT(1) FROM {_options.SchemaName}.Locks where Name = @name",new { name = _lockName }) != 0;
        }
    }
}