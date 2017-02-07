using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using Lucene.Net.Store;
using Lucene.Net.Support;
using LuceneNetSqlDirectory.Helpers;

namespace LuceneNetSqlDirectory
{
    public sealed class SqlServerDirectory : Directory
    {
        private readonly SqlConnection _connection;
        private readonly Options _options;

        public SqlServerDirectory(SqlConnection connection, Options options)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            _connection = connection;
            _options = options;
            ValidateConfiguration();
            SetLockFactory(new SqlServerLockFactory(connection, options));
        }

        private void ValidateConfiguration()
        {
            { // Connection management should be done outside this library
                if (_connection.State != ConnectionState.Open)
                {
                    throw new ConfigurationErrorsException($"The connection is not open. SQLServerDirectory does not perform any connection management (opening, disposing or closing), this should be handled by the calling application.");
                }
            }
            { // Validate if the required database structure is available
                var tables = _connection.GetSchema("Tables");
                var alltablesAreAvailable = tables.Select($"(TABLE_NAME = 'Locks' OR TABLE_NAME = 'FileMetaData' OR TABLE_NAME = 'FileContents' ) AND ( TABLE_SCHEMA = '{ SqlHelper.RemoveBrackets(_options.SchemaName)}')").Count() == 3;
                if (false == alltablesAreAvailable)
                {
                    throw new ConfigurationErrorsException($"The database structure required for the SQLServerDirectory are not available in database : '{_connection.Database}'");
                }
            }
            { // Validate if MARS is enabled because we need this to read efficiently!
                var connectionString = new SqlConnectionStringBuilder(_connection.ConnectionString);
                if (connectionString.MultipleActiveResultSets == false)
                {
                    throw new ConfigurationErrorsException($"The given connection does not have 'MultipleActiveResultSets' enabled. SQLServerDirectory requires this feature in order to read efficiently from the database using multiple readers. Please add the following in the connectionstring that is used for the given connection : 'MultipleActiveResultSets=True;'");

                }
            } // We require MS SQLServer 2008 or higher (the lowest dependency today is DateTime2 type)
            {
                if (Convert.ToInt16(_connection.ServerVersion.Split('.')[0]) < 10)
                {
                    throw new ConfigurationErrorsException($"The database server used for the SQLServerDirectory should be at least a MSSQLServer 2008. Required version: 10, current version: {_connection.ServerVersion}");
                }
            }
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
            var lastTouched = _connection.ExecuteScalar<DateTimeOffset>($"SElECT TOP 1 LastTouchedTimestamp FROM {_options.SchemaName}.FileMetaData WHERE name = @name", new { name });
            return lastTouched.UtcTicks;
        }

        public override void TouchFile(string name)
        {
            _connection.Execute($"UPDATE {_options.SchemaName}.FileMetaData SET LastTouchedTimestamp = SYSUTCDATETIME() WHERE name = @name ", new { name });
        }

        public override void DeleteFile(string name)
        {
            SqlServerIndexOutput runningOutput;
            if (_runningOutputs.TryRemove(name, out runningOutput))
            {
                runningOutput.Dispose();
            }
            SqlServerIndexInput runningInput;
            if (_runningInputs.TryRemove(name, out runningInput))
            {
                runningInput.Dispose();
            }

            _connection.Execute($"UPDATE {_options.SchemaName}.FileMetaData SET name = NEWID(), IsDeleted = 1 WHERE name = @name", new { name });
            _connection.Execute($"UPDATE {_options.SchemaName}.FileContents SET name = NEWID(), IsDeleted = 1 WHERE name = @name", new { name });
        }

        private readonly ConcurrentDictionary<string, SqlServerIndexInput> _runningInputs = new ConcurrentDictionary<string, SqlServerIndexInput>();
        private readonly ConcurrentDictionary<string, SqlServerIndexOutput> _runningOutputs = new ConcurrentDictionary<string, SqlServerIndexOutput>();

        public override long FileLength(string name)
        {
            return _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name });
        }

        public override IndexOutput CreateOutput(string name)
        {
            SqlServerIndexOutput runningOutput;
            if (_runningOutputs.TryRemove(name, out runningOutput))
            {
                runningOutput.Dispose();
            }

            if (0 == _connection.ExecuteScalar<int>($"SELECT COUNT(0) FROM {_options.SchemaName}.FileContents WHERE Name = @name", new { name }))
            {
                _connection.Execute($"INSERT INTO {_options.SchemaName}.FileContents (Name,Content) VALUES (@name,@content)", new { name, content = new byte[0] });
            }
            if (0 == _connection.ExecuteScalar<int>($"SELECT COUNT(0) FROM {_options.SchemaName}.FileMetaData WHERE Name = @name", new { name }))
            {
                _connection.Execute($"INSERT INTO {_options.SchemaName}.FileMetaData (Name,LastTouchedTimestamp) VALUES (@name,SYSUTCDATETIME())", new { name });
            }

            var result = new SqlServerIndexOutput(_connection, name, _options);
            _runningOutputs.TryAdd(name, result);

            return result;
        }

        public override IndexInput OpenInput(string name)
        {
            SqlServerIndexInput runningInput;
            if (_runningInputs.TryRemove(name, out runningInput))
            {
                runningInput.Dispose();
            }

            var result = new SqlServerIndexInput(_connection, name, _options);
            _runningInputs.TryAdd(name, result);
            return result;
        }

        protected override void Dispose(bool disposing)
        {
            _runningInputs.Values.ForEach(z => z.Dispose());
            _runningOutputs.Values.ForEach(z => z.Dispose());
            _connection.Dispose();
        }
    }
}