using System;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;
using LuceneNetSqlDirectory.Helpers;

namespace LuceneNetSqlDirectory
{
    class SqlServerIndexOutput : BufferedIndexOutput
    {
        private readonly SqlConnection _connection;
        private readonly string _name;
        private readonly Options _options;
        private SqlCommand _updateCommand;
        private SqlParameter _parameterName;
        private SqlParameter _parameterData;
        private SqlParameter _parameterIndex;
        private SqlParameter _parameterLen;
        public SqlServerIndexOutput(SqlConnection connection, string name, Options options)
        {
            _connection = connection;
            _name = name;
            _options = options;
        }

        public override void FlushBuffer(byte[] b, int offset, int len)
        {
            var segment = new byte[len];
            Buffer.BlockCopy(b, offset, segment, 0, len);

            _updateCommand = _updateCommand ?? new SqlCommand($"UPDATE {_options.SchemaName}.[FileContents] SET [Content].WRITE(@chunk, @index, @len) WHERE [Name] = @name", _connection);
            _parameterName = _parameterName ?? _updateCommand.Parameters.Add("@name", SqlDbType.NVarChar);
            _parameterData = _parameterData ?? _updateCommand.Parameters.Add("@chunk", SqlDbType.VarBinary, -1);
            _parameterIndex = _parameterIndex ?? _updateCommand.Parameters.Add("index", SqlDbType.BigInt);
            _parameterLen = _parameterLen ?? _updateCommand.Parameters.Add("len", SqlDbType.BigInt);

            _parameterName.Value = _name;
            _parameterData.Value = segment;
            _parameterIndex.Value = FilePointer - len;
            _parameterLen.Value = len;

            _updateCommand.ExecuteNonQuery();
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            _updateCommand?.Dispose();
            _updateCommand = null;
        }

        public override long Length => _connection.ExecuteScalar<long>($"SELECT DATALENGTH(Content) FROM {_options.SchemaName}.FileContents WHERE name = @name", new { name = _name });
    }
}