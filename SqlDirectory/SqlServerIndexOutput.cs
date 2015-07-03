using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    class SqlServerIndexOutput : IndexOutput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private long _pointer = 0;

        public SqlServerIndexOutput(SqlConnection connection, string name)
        {
            _connection = connection;
            _name = name;
        }

        public override void WriteByte(byte b)
        {
            WriteBytes(new[] { b }, 0, 1);
        }

        private bool? isFirst = true;


        public override void WriteBytes(byte[] b, int offset, int length)
        {
            if (isFirst == null)
            {
                isFirst = Length == 0;
            }
            if (length == 1)
            {
                _connection.Write(new[] { b[0] }, (int)_pointer, 1, _name, isFirst.Value);
                isFirst = false;
            }
            else if (length > 1)
            {
                var segment = new byte[length];
                Buffer.BlockCopy(b, offset, segment, 0, length);
                _connection.Write(segment, (int)_pointer, length, _name, isFirst.Value);
                isFirst = false;
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

        public override long FilePointer
        {
            get { return _pointer; }
        }

        public override long Length
        {
            get { return _connection.ExecuteScalar<long>("SELECT DATALENGTH(Content) FROM dbo.FileContents WHERE name = @name", new { name = _name }); }
        }
    }
}