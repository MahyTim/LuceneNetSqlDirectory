using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    internal class SqlServerIndexInput : IndexInput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly Options _options;
        private long _position;

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

            _connection.ReadBytes(_name, _position, b, offset, len, _options.SchemaName);

            _position += len;
        }

        protected override void Dispose(bool disposing)
        {
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