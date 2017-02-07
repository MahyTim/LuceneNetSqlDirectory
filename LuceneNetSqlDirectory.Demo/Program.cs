using System;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace LuceneNetSqlDirectory.Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var connection = new SqlConnection(@"MultipleActiveResultSets=True;Data Source=onboarding;Initial Catalog=TestLucene;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False"))
            {
                connection.Open();
                SqlServerDirectory.ProvisionDatabase(connection, schemaName: "[search]", dropExisting: true);
            }

            var t1 = Task.Factory.StartNew(Do);
            //var t2 = Task.Factory.StartNew(Do);
            //var t3 = Task.Factory.StartNew(Do);
            t1.Wait();
            //Task.WaitAll(t1, t2, t3);
        }

        static void LockCanBeReleased()
        {
            using (var connection = new SqlConnection(@"MultipleActiveResultSets=True;Data Source=onboarding;Initial Catalog=TestLucene;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False"))
            {
                connection.Open();
                var directory = new SqlServerDirectory(connection, new Options() { SchemaName = "[search]", LockTimeoutInSeconds = 1*60 });

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

        static void Do()
        {
            //var directory = new SimpleFSDirectory(new DirectoryInfo(@"c:\temp\lucene"));
            using (var connection = new SqlConnection(@"MultipleActiveResultSets=True;Data Source=onboarding;Initial Catalog=TestLucene;Integrated Security=True;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False"))
            {
                connection.Open();
               
            }
        }


        

    }
    
}
