using System;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    class SqlServerDirectory : Directory
    {
        private readonly SqlConnection _connection;
        public SqlServerDirectory(SqlConnection connection)
        {
            _connection = connection;
            base.SetLockFactory(new SqlServerLockFactory(connection));
        }

        public override string[] ListAll()
        {
            return _connection.Query<string>("SELECT Name FROM dbo.FileMetaData").ToArray();
        }

        public override bool FileExists(string name)
        {
            return _connection.ExecuteScalar<int>("SELECT COUNT(0) FROM dbo.FileMetaData WHERE Name = @name", new { name }) != 0;
        }

        public override long FileModified(string name)
        {
            var lastTouched = _connection.ExecuteScalar<DateTimeOffset>("SElECT TOP 1 LastTouched FROM dbo.FileMetaData WHERE name = @name", new { name });
            return lastTouched.UtcTicks;
        }

        public override void TouchFile(string name)
        {
            _connection.Execute("UPDATE dbo.FileMetaData SET LastTouched = SYSUTCDATETIME() WHERE name = @name ", new { name });
        }

        public override void DeleteFile(string name)
        {
            _connection.Execute("DELETE FROM dbo.FileMetaData WHERE name = @name", new { name });
            _connection.Execute("DELETE FROM dbo.FileContents WHERE name = @name", new { name });
        }

        public override long FileLength(string name)
        {
            return _connection.ExecuteScalar<long>("SELECT DATALENGTH(Content) FROM dbo.FileContents WHERE name = @name", new { name });
        }

        public override IndexOutput CreateOutput(string name)
        {
            _connection.Execute("INSERT INTO dbo.FileMetaData (Name,LastTouched) VALUES (@name,SYSUTCDATETIME())", new { name });
            _connection.Execute("INSERT INTO dbo.FileContents (Name,Content) VALUES (@name,null)", new { name });
            return new SqlServerIndexOutput(_connection, name);
        }

        public override IndexInput OpenInput(string name)
        {
            return new SqlServerIndexInput(_connection, name);
        }

        protected override void Dispose(bool disposing)
        {
            _connection.Dispose();
        }
    }
}