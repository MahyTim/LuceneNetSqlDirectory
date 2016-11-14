using System;
using System.Data;
using System.Data.SqlClient;

namespace LuceneNetSqlDirectory.Helpers
{
    class SqlServerStreamingReader : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly string _schemaName;
        private SqlCommand _command;
        private SqlDataReader _reader;
        private long _currentPosition;

        public SqlServerStreamingReader(SqlConnection connection, string name, string schemaName)
        {
            _connection = connection;
            _name = name;
            _schemaName = schemaName;
            Initialize();
        }

        public void ReadBytes(long position, byte[] b, int offset, int len)
        {
            if (false == _reader.HasRows)
            {
                return;
            }
            if (position < _currentPosition)
            {
                Initialize();
            }
            if (false == _reader.IsDBNull(0))
            {
                _reader.GetBytes(0, position, b, offset, len);
            }
            _currentPosition = position + len;
        }

        private void Initialize()
        {
            if (_command != null)
            {
                _reader.Dispose();
                _command.Dispose();
            }
            _command = new SqlCommand($"SELECT Content FROM {_schemaName}.[FileContents] WHERE Name = @name", _connection);
            _command.Parameters.AddWithValue("name", _name);
            _reader = _command.ExecuteReader(CommandBehavior.SequentialAccess);
            _reader.Read();
        }

        public void Dispose()
        {
            _reader.Dispose();
            _command.Dispose();
        }
    }
}