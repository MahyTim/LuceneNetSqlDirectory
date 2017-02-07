using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LuceneNetSqlDirectory.Tests
{
    [TestClass]
    public class LockCanBeReleasedTests : LuceneTestCase
    {
        [TestMethod]
        public void Test_Lock_Is_Released()
        {
            var directory = new SqlServerDirectory(Connection, new Options() { LockTimeoutInSeconds = 3 });

            new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30),
                        !IndexReader.IndexExists(directory),
                        new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));

            IndexWriter indexWriter = null;
            while (indexWriter == null)
            {
                try
                {
                    indexWriter = new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), !IndexReader.IndexExists(directory),
                        new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
                }
                catch (LockObtainFailedException)
                {
                    Console.WriteLine("Lock is taken, waiting for timeout...{0}", DateTime.Now);
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
