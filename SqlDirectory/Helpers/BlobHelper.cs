using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SqlDirectory
{
    class SqlServerStreamingWriter : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly string _schemaName;
        private readonly string _name;
        private ByteWriter _buffer = new ByteWriter(4096);
        private SqlCommand _updateCommand;
        private SqlParameter _parameterName;
        private SqlParameter _parameterData;
        private SqlParameter _parameterIndex;
        private SqlParameter _parameterLen;
        public SqlServerStreamingWriter(SqlConnection connection, string schemaName, string name)
        {
            _connection = connection;
            _schemaName = schemaName;
            _name = name;
        }
        private void Write(byte[] buffer, int index, int len, bool isFirstWrite)
        {
            if (isFirstWrite)
            {
                using (var cmdFirstChunk = new SqlCommand($"UPDATE {_schemaName}.[FileContents] SET [Content] = @firstChunk WHERE [Name] = @name", _connection))
                {
                    cmdFirstChunk.Parameters.AddWithValue("name", _name);
                    var paramChunk = new SqlParameter("@firstChunk", SqlDbType.VarBinary, -1);
                    cmdFirstChunk.Parameters.Add(paramChunk);
                    paramChunk.Value = buffer;
                    cmdFirstChunk.ExecuteNonQuery();
                }
            }
            _updateCommand = _updateCommand ?? new SqlCommand($"UPDATE {_schemaName}.[FileContents] SET [Content].WRITE(@chunk, @index, @len) WHERE [Name] = @name", _connection);
            _parameterName = _parameterName ?? _updateCommand.Parameters.Add("@name", SqlDbType.NVarChar);
            _parameterData = _parameterData ?? _updateCommand.Parameters.Add("@chunk", SqlDbType.VarBinary, -1);
            _parameterIndex = _parameterIndex ?? _updateCommand.Parameters.Add("index", SqlDbType.BigInt);
            _parameterLen = _parameterLen ?? _updateCommand.Parameters.Add("len", SqlDbType.BigInt);

            _parameterName.Value = _name;
            _parameterData.Value = buffer;
            _parameterIndex.Value = index;
            _parameterLen.Value = len;

            _updateCommand.ExecuteNonQuery();
        }

        public void Add(long pointer, byte[] segment)
        {
            _buffer.Add(pointer, segment);
        }

        public void Write(Func<long> length)
        {
            var segments = _buffer.GetSegments();
            if (segments.Any())
            {
                var isFirst = length() == 0;
                foreach (var segment in segments)
                {
                    Write(segment.Buffer, (int)segment.Position, segment.Buffer.Length, isFirst);
                    isFirst = false;
                }
                _buffer = new ByteWriter(4082);
            }
        }

        public void Dispose()
        {
            _updateCommand.Dispose();
        }
    }
}