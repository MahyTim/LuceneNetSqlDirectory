using System;

namespace SqlDirectory
{
    internal class ReInitializableLazy<T> : IDisposable
    {
        private Lazy<T> _lazy;
        private readonly Func<T> _initializer;

        public ReInitializableLazy(Func<T> action)
        {
            _initializer = action;
            _lazy = new Lazy<T>(action);
        }

        public T Result => _lazy.Value;

        public void ReInitialize()
        {
            _lazy = new Lazy<T>(_initializer);
        }

        public void Dispose()
        {
            if (_lazy.IsValueCreated)
            {
                (_lazy.Value as IDisposable)?.Dispose();
            }
        }
    }
}