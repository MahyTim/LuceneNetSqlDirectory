using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace LuceneNetSqlDirectory.Tests
{
    [TestClass]
    public class TestDemo : LuceneTestCase
    {
        [TestMethod]
        public virtual void TestDemo_Renamed()
        {
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_30);
            // Store the index in memory:
            using (var directory = new SqlServerDirectory(Connection, new Options()))
            {
                // To store an index on disk, use this instead:
                //Directory directory = FSDirectory.open("/tmp/testindex");
                using (var iwriter = new IndexWriter(directory, analyzer, true, new IndexWriter.MaxFieldLength(25000)))
                {
                    var doc = new Document();
                    var text = "This is the text to be indexed.";
                    doc.Add(new Field("fieldname", text, Field.Store.YES, Field.Index.ANALYZED));
                    iwriter.AddDocument(doc);
                }

                // Now search the index:
                using (IndexSearcher isearcher = new IndexSearcher(directory, true))
                {
                    // read-only=true
                    // Parse a simple query that searches for "text":
                    QueryParser parser = new QueryParser(Version.LUCENE_30, "fieldname", analyzer);
                    Query query = parser.Parse("text");
                    ScoreDoc[] hits = isearcher.Search(query, null, 1000).ScoreDocs;
                    Assert.AreEqual(1, hits.Length);
                    // Iterate through the results:
                    for (int i = 0; i < hits.Length; i++)
                    {
                        Document hitDoc = isearcher.Doc(hits[i].Doc);
                        Assert.AreEqual(hitDoc.Get("fieldname"), "This is the text to be indexed.");
                    }
                }
            }
        }
    }
}