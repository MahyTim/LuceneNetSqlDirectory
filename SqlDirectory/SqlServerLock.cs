using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    internal class SqlServerLock : Lock
    {
        private readonly SqlConnection _connection;
        private readonly string _lockName;

        public SqlServerLock(SqlConnection connection, string lockName)
        {
            this._connection = connection;
            this._lockName = lockName;
        }

        public override bool Obtain()
        {
            if (IsLocked())
                return false;
            _connection.Execute("INSERT INTO dbo.Locks (Name) VALUES (@name)", new { name = _lockName });
            return true;
        }

        public override void Release()
        {
            _connection.Execute("DELETE FROM dbo.Locks Where Name = @name", new { name = _lockName });
        }

        public override bool IsLocked()
        {
            return _connection.ExecuteScalar<int>(@"SELECT COUNT(1) FROM dbo.Locks where Name = @name",
                new { name = _lockName }) != 0;
        }
    }
}