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

        private SqlServerStreamingReader _dataReader;

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
            _dataReader = _dataReader ?? new SqlServerStreamingReader(_connection, _name, _options.SchemaName);
            if (b.Length == 0)
                return;

            _dataReader.ReadBytes(_position, b, offset, len);

            _position += len;
        }

        protected override void Dispose(bool disposing)
        {
            if (_dataReader != null)
            {
                _dataReader.Dispose();
                _dataReader = null;
            }
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