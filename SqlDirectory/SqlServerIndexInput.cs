using System;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using Lucene.Net.Store;

namespace SqlDirectory
{
    internal class Disposer<T> : IDisposable where T : IDisposable
    {
        private readonly IDisposable[] _others;
        public T Data { get; private set; }

        public Disposer(T data, params IDisposable[] others)
        {
            _others = others;
            Data = data;
        }

        public void Dispose()
        {
            Data.Dispose();
            foreach (var disposable in _others)
            {
                disposable.Dispose();
            }
        }
    }

    internal class ReInitializableLazy<T> : IDisposable
    {
        private Lazy<T> _lazy;
        private Func<T> _initializer;

        public ReInitializableLazy(Func<T> action)
        {
            _initializer = action;
            _lazy = new Lazy<T>(action);
        }

        public T Result { get { return _lazy.Value; } }

        public void ReInitialize()
        {
            _lazy = new Lazy<T>(_initializer);
        }

        public void Dispose()
        {
            if (_lazy.IsValueCreated)
            {
                var d = _lazy.Value as IDisposable;
                if (d != null)
                    d.Dispose();
            }
        }
    }

    internal class SqlServerIndexInput : IndexInput
    {
        private readonly SqlConnection _connection;
        private readonly ReInitializableLazy<Disposer<SqlDataReader>> _reader;
        private readonly string _name;
        private long _position;

        internal SqlServerIndexInput(SqlConnection connection, string name)
        {
            _connection = connection;
            _reader = new ReInitializableLazy<Disposer<SqlDataReader>>(() =>
            {
                var command = new SqlCommand("SELECT Content FROM FileContents WHERE Name = @name", connection);
                command.Parameters.AddWithValue("name", name);
                var reader = command.ExecuteReader();
                reader.Read();
                return new Disposer<SqlDataReader>(reader, command);
            });
            _name = name;
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

            if (_reader.Result.Data.IsClosed)
            {
                _reader.ReInitialize();
            }
            if (_reader.Result.Data.HasRows)
            {
                if (false == _reader.Result.Data.IsDBNull(0))
                {
                    _reader.Result.Data.GetBytes(0, _position, b, offset, len);
                }
            }
            _position += len;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _reader.Dispose();
        }

        public override void Seek(long pos)
        {
            if (pos <= _position)
            {
                _reader.ReInitialize();
            }
            _position = pos;
        }

        public override long Length()
        {
            return _connection.ExecuteScalar<long>("SELECT DATALENGTH(Content) FROM dbo.FileContents WHERE name = @name", new { name = _name });
        }

        public override long FilePointer
        {
            get { return _position; }
        }
    }
}