using System;
using System.Data.SqlClient;
using System.Net.Sockets;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    internal class SqlServerLockFactory : LockFactory
    {
        private readonly SqlConnection _connection;

        internal SqlServerLockFactory(SqlConnection connection)
        {
            _connection = connection;
        }

        public override Lock MakeLock(string lockName)
        {
            return new SqlServerLock(_connection, lockName);
        }

        public override void ClearLock(string lockName)
        {
            _connection.Execute("DELETE FROM dbo.Locks Where Name = @name", new { name = lockName });
        }
    }
}