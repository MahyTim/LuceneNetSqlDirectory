using System.Data;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;
using LuceneNetSqlDirectory.Helpers;

namespace LuceneNetSqlDirectory
{
    internal class SqlServerIndexInput : IndexInput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly Options _options;
        private long _position;

        private SqlCommand _command;
        private SqlDataReader _reader;

        internal SqlServerIndexInput(SqlConnection connection, string name, Options options)
        {
            _connection = connection;
            _name = name;
            _options = options;
        }

        public override byte ReadByte()
        {
            var buffer = new byte[1];
            ReadBytes(buffer, 0, 1);
            return buffer[0];
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            if (b.Length == 0)
                return;

            if (_command == null || _reader == null || _reader.IsClosed || offset < _position)
            {
                _reader?.Dispose();
                _command?.Dispose();
                _command = new SqlCommand($"SELECT Content FROM {_options.SchemaName}.[FileContents] WHERE Name = @name", _connection);
                _command.Parameters.AddWithValue("name", _name);
                _reader = _command.ExecuteReader(CommandBehavior.SequentialAccess);
                _reader.Read();
            }
            if (false == _reader.HasRows)
            {
                return;
            }
            if (false == _reader.IsDBNull(0))
            {
                _reader.GetBytes(0, _position, b, offset, len);
            }
            _position += len;
        }

        protected override void Dispose(bool disposing)
        {
            _reader?.Dispose();
            _command?.Dispose();
            _reader = null;
            _command = null;
        }

        public override void Seek(long pos)
        {
            _position = pos;
        }

        public override long Length()
        {
            return _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name = _name });
        }

        public override long FilePointer => _position;
    }
}