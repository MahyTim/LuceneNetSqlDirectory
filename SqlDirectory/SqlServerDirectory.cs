using System;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    public sealed class SqlServerDirectory : Directory
    {
        private readonly SqlConnection _connection;
        private readonly Options _options;

        public SqlServerDirectory(SqlConnection connection, Options options)
        {
            _connection = connection;
            _options = options;
            SetLockFactory(new SqlServerLockFactory(connection, options));
        }

        public static void ProvisionDatabase(SqlConnection connection, string schemaName = "[dbo]", bool dropExisting = false)
        {
            if (dropExisting)
            {
                Database.Tables.ForEach(z => connection.DropTableIfExists(schemaName, z));
            }
            if (false == connection.SchemaExists(schemaName))
            {
                connection.Execute($"CREATE SCHEMA {schemaName}");
            }
            Database.Structure(schemaName).ForEach(z => connection.Execute(z));
        }

        public override string[] ListAll()
        {
            return _connection.Query<string>($"SELECT Name FROM {_options.SchemaName}.FileMetaData").ToArray();
        }

        public override bool FileExists(string name)
        {
            return _connection.ExecuteScalar<int>($"SELECT COUNT(0) FROM {_options.SchemaName}.FileMetaData WHERE Name = @name", new { name }) != 0;
        }

        public override long FileModified(string name)
        {
            var lastTouched = _connection.ExecuteScalar<DateTimeOffset>($"SElECT TOP 1 LastTouched FROM {_options.SchemaName}.FileMetaData WHERE name = @name", new { name });
            return lastTouched.UtcTicks;
        }

        public override void TouchFile(string name)
        {
            _connection.Execute($"UPDATE {_options.SchemaName}.FileMetaData SET LastTouched = SYSUTCDATETIME() WHERE name = @name ", new { name });
        }

        public override void DeleteFile(string name)
        {
            _connection.Execute($"DELETE FROM {_options.SchemaName}.FileMetaData WHERE name = @name", new { name });
            _connection.Execute($"DELETE FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name });
        }

        public override long FileLength(string name)
        {
            return _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name });
        }

        public override IndexOutput CreateOutput(string name)
        {
            if (0 == _connection.ExecuteScalar<int>($"SELECT COUNT(0) FROM {_options.SchemaName}.FileContents WHERE Name = @name", new { name }))
            {
                _connection.Execute($"INSERT INTO {_options.SchemaName}.FileContents (Name,Content) VALUES (@name,null)", new { name });
            }
            if (0 == _connection.ExecuteScalar<int>($"SELECT COUNT(0) FROM {_options.SchemaName}.FileMetaData WHERE Name = @name", new { name }))
            {
                _connection.Execute($"INSERT INTO {_options.SchemaName}.FileMetaData (Name,LastTouched) VALUES (@name,SYSUTCDATETIME())", new { name });
            }
            return new SqlServerIndexOutput(_connection, name, _options);
        }

        public override IndexInput OpenInput(string name)
        {
            return new SqlServerIndexInput(_connection, name, _options);
        }

        protected override void Dispose(bool disposing)
        {
            _connection.Dispose();
        }
    }
}