using System;
using System.Text;
using System.Threading;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using LuceneNetSqlDirectory.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LuceneNetSqlDirectory.Tests
{
    [TestClass]
    public class DemoUsage : LuceneTestCase
    {
        [TestMethod]
        public void Demo_Optimize_Test()
        {
            for (int i = 0; i < 3; i++)
            {
                var directory = new SqlServerDirectory(Connection, new Options());
                var indexWriter = new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), !IndexReader.IndexExists(directory), new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
                indexWriter.SetMergeScheduler(new ConcurrentMergeScheduler());
                indexWriter.SetMaxBufferedDocs(1000);

                for (int iDoc = 0; iDoc < 1000 * 10; iDoc++)
                {
                    Document doc = new Document();
                    doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.NO));
                    doc.Add(new Field("Title", "dog " + " microsoft rules", Field.Store.NO, Field.Index.ANALYZED,
                        Field.TermVector.NO));
                    doc.Add(new Field("Body", "dog " + " microsoft rules", Field.Store.NO, Field.Index.ANALYZED,
                        Field.TermVector.NO));
                    indexWriter.AddDocument(doc);
                }
                indexWriter.Flush(true, true, true);
                indexWriter.Optimize(true);
                indexWriter.Dispose(true);

                var searcher = new IndexSearcher(directory);
                Console.WriteLine("Number of docs: {0}", searcher.IndexReader.NumDocs());
                SearchForPhrase(searcher, "microsoft", 999);
                searcher.Dispose();
            }
        }

        [TestMethod]
        public void Demo_Usage_Test()
        {
            var directory = new SqlServerDirectory(Connection, new Options());

            for (int outer = 0; outer < 10; outer++)
            {
                IndexWriter indexWriter = null;
                while (indexWriter == null)
                {
                    try
                    {
                        indexWriter = new IndexWriter(directory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30),
                            !IndexReader.IndexExists(directory),
                            new Lucene.Net.Index.IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
                    }
                    catch (LockObtainFailedException)
                    {
                        Console.WriteLine("Lock is taken, waiting for timeout...");
                        Thread.Sleep(1000);
                    }
                }
                ;
                Console.WriteLine("IndexWriter lock obtained, this process has exclusive write access to index");
                indexWriter.SetRAMBufferSizeMB(100.0);
                //indexWriter.SetInfoStream(new StreamWriter(Console.OpenStandardOutput()));
                indexWriter.SetMergeScheduler(new SerialMergeScheduler());
                indexWriter.SetMaxBufferedDocs(500);

                for (int iDoc = 0; iDoc < 1000; iDoc++)
                {
                    Document doc = new Document();
                    doc.Add(new Field("id", DateTime.Now.ToFileTimeUtc().ToString(), Field.Store.YES,
                        Field.Index.ANALYZED, Field.TermVector.NO));
                    doc.Add(new Field("Title", "dog " + GeneratePhrase(50), Field.Store.NO, Field.Index.ANALYZED,
                        Field.TermVector.NO));
                    doc.Add(new Field("Body", "dog " + GeneratePhrase(50), Field.Store.NO, Field.Index.ANALYZED,
                        Field.TermVector.NO));
                    indexWriter.AddDocument(doc);
                }

                Console.WriteLine("Total docs is {0}", indexWriter.NumDocs());

                Console.Write("Flushing and disposing writer...");
                indexWriter.Flush(true, true, true);
                //indexWriter.Optimize();
                indexWriter.Commit();
                indexWriter.Dispose();
            }

            var searcher = new IndexSearcher(directory);
            Console.WriteLine("Number of docs: {0}", searcher.IndexReader.NumDocs());
            SearchForPhrase(searcher, "microsoft", 2);
        }

        static void SearchForPhrase(IndexSearcher searcher, string phrase, int minNumberOfHits)
        {
            using (new AutoStopWatch($"Search for {phrase}"))
            {
                Lucene.Net.QueryParsers.QueryParser parser = new Lucene.Net.QueryParsers.QueryParser(Lucene.Net.Util.Version.LUCENE_30, "Body", new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30));
                Lucene.Net.Search.Query query = parser.Parse(phrase);

                var hits = searcher.Search(new TermQuery(new Term("Title", "find me")), 100);

                hits = searcher.Search(query, 100);
                Console.WriteLine("Found {0} results for {1}", hits.TotalHits, phrase);
                Assert.IsTrue(hits.TotalHits > minNumberOfHits);
            }
        }

        static readonly Random Random = new Random((int)DateTime.Now.Ticks);
        static readonly string[] SampleTerms =
        {
            "dog","cat","car","horse","door","tree","chair","microsoft","apple","adobe","google","golf","linux","windows","firefox","mouse","hornet","monkey","giraffe","computer","monitor",
            "steve","fred","lili","albert","tom","shane","gerald","chris",
            "love","hate","scared","fast","slow","new","old"
        };

        private static string GeneratePhrase(int maxTerms)
        {
            StringBuilder phrase = new StringBuilder();
            int nWords = 2 + Random.Next(maxTerms);
            for (int i = 0; i < nWords; i++)
            {
                phrase.AppendFormat(" {0} {1}", SampleTerms[Random.Next(SampleTerms.Length)], Random.Next(32768).ToString());
            }
            return phrase.ToString();
        }
    }
}