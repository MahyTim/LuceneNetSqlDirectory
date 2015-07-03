using System;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    class SqlServerIndexOutput : IndexOutput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly Options _options;
        private long _pointer;

        public SqlServerIndexOutput(SqlConnection connection, string name, Options options)
        {
            _connection = connection;
            _name = name;
            _options = options;
        }

        public override void WriteByte(byte b)
        {
            WriteBytes(new[] { b }, 0, 1);
        }

        private bool? _isFirst = true;


        public override void WriteBytes(byte[] b, int offset, int length)
        {
            if (_isFirst == null)
            {
                _isFirst = Length == 0;
            }
            if (length == 1)
            {
                _connection.Write(new[] { b[0] }, (int)_pointer, 1, _name, _isFirst.Value, _options.SchemaName);
                _isFirst = false;
            }
            else if (length > 1)
            {
                var segment = new byte[length];
                Buffer.BlockCopy(b, offset, segment, 0, length);
                _connection.Write(segment, (int)_pointer, length, _name, _isFirst.Value, _options.SchemaName);
                _isFirst = false;
            }
            _pointer += length;
        }

        public override void Flush()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }

        public override void Seek(long pos)
        {
            _pointer = pos;
        }

        public override long FilePointer => _pointer;

        public override long Length => _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name = _name });
    }
}