using System;
using System.Diagnostics;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;

namespace LuceneNetSqlDirectory
{
    public class AutoStopWatch : IDisposable
    {
        private readonly Stopwatch _stopwatch;
        private readonly string _message;
        public AutoStopWatch(string message)
        {
            _message = message;
            Console.WriteLine("{0} starting ", message);
            _stopwatch = Stopwatch.StartNew();
        }


        #region IDisposable Members
        public void Dispose()
        {

            _stopwatch.Stop();
            long ms = _stopwatch.ElapsedMilliseconds;

            Console.WriteLine("{0} Finished {1} ms", _message, ms);
        }
        #endregion
    }
}