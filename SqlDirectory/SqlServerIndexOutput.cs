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

        private List<PendingWrite> _pendingWrites = new List<PendingWrite>();

        class PendingWrite
        {
            public byte[] Buffer { get; set; }
            public int Length { get; set; }
            public long Position { get; set; }
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            var segment = new byte[length];
            Buffer.BlockCopy(b, offset, segment, 0, length);
            _pendingWrites.Add(new PendingWrite()
            {
                Buffer = segment,
                Length = length,
                Position = _pointer
            });
            _pointer += length;
        }

        public override void Flush()
        {
            if (_pendingWrites.Any())
            {
                var isFirst = Length == 0;
                foreach (var pendingWrite in _pendingWrites)
                {
                    _connection.Write(pendingWrite.Buffer, (int)pendingWrite.Position, pendingWrite.Length, _name, isFirst, _options.SchemaName);
                    isFirst = false;
                }
                _pendingWrites.Clear();
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