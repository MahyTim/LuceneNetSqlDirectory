using System;
using System.Data.SqlClient;
using Lucene.Net.Store;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Directory = System.IO.Directory;

namespace LuceneNetSqlDirectory.Tests
{
    [TestClass]
    public class LuceneTestCase
    {
        [TestInitialize]
        public virtual void TestInitialize()
        {
            Connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["databaseForTests"].ConnectionString);
            Connection.Open();
            SqlServerDirectory.ProvisionDatabase(Connection, schemaName: new Options().SchemaName, dropExisting: true);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Connection.Dispose();
        }

        protected SqlConnection Connection { get; private set; }
    }
}
