using System;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;
using LuceneNetSqlDirectory.Helpers;

namespace LuceneNetSqlDirectory
{
    class SqlServerIndexOutput : IndexOutput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly Options _options;
        private long _pointer;
        private SqlServerStreamingWriter _writer;

        public SqlServerIndexOutput(SqlConnection connection, string name, Options options)
        {
            _connection = connection;
            _name = name;
            _options = options;
            _writer = new SqlServerStreamingWriter(connection, options.SchemaName, name);
        }

        public override void WriteByte(byte b)
        {
            WriteBytes(new[] { b }, 0, 1);
        }

        public override void WriteBytes(byte[] b, int offset, int length)
        {
            var segment = new byte[length];
            Buffer.BlockCopy(b, offset, segment, 0, length);
            _writer.Add(_pointer, segment);
            _pointer += length;
        }

        public override void Flush()
        {
            _writer.Write(() => Length);
        }

        protected override void Dispose(bool disposing)
        {
            Flush();
            if (disposing)
                _writer.Dispose();
        }

        public override void Seek(long pos)
        {
            _pointer = pos;
        }

        public override long FilePointer => _pointer;

        public override long Length => _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name = _name });
    }
}