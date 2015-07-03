using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
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

        private ByteWriter _buffer = new ByteWriter(4082);

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            var segment = new byte[length];
            Buffer.BlockCopy(b, offset, segment, 0, length);
            _buffer.Add(_pointer, segment);
            _pointer += length;
        }

        public override void Flush()
        {
            var segments = _buffer.GetSegments();
            if (segments.Any())
            {
                var isFirst = Length == 0;
                foreach (var segment in segments)
                {
                    _connection.Write(segment.Buffer, (int)segment.Position, segment.Buffer.Length, _name, isFirst, _options.SchemaName);
                    isFirst = false;
                }
                _buffer = new ByteWriter(4082);
            }
        }

        protected override void Dispose(bool disposing)
        {
            Flush();
        }

        public override void Seek(long pos)
        {
            _pointer = pos;
        }

        public override long FilePointer => _pointer;

        public override long Length => _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name = _name });
    }
}