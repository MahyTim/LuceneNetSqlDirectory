using System;

namespace SqlDirectory
{
    internal class Disposer<T> : IDisposable where T : IDisposable
    {
        private readonly IDisposable[] _others;
        public T Data { get; }

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
}