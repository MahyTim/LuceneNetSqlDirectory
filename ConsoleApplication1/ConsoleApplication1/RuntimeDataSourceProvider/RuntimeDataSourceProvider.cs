
/*
 Contains a custom System.Data.Common.DbProviderFactory 'RuntimeDataSourceProviderFactory' 
 *Can be used as a DataSource inside DataDriven MsTest.
 *Usage:
 *app.config:
 *  <system.data>
    <DbProviderFactories>
      <add name="RuntimeDataSourceProvider"
           invariant="RuntimeDataSourceProvider"
           description="DataDriven Unit Testing RuntimeDataSourceProvider"
           type="RuntimeDataSourceProvider.RuntimeDataSourceProviderFactory, <yourassemblyname>"/>
    </DbProviderFactories>
  </system.data>
 * Unit Test code:
 *   [DataSource(
        "RuntimeDataSourceProvider", 
        "UnitTestProject1.UnitTest1",
        "GetExampleDataList", DataAccessMethod.Sequential)]
        [TestMethod]
        public void TestExampleListResultIsNumberSquare()
        {
            var value = (ExampleData) TestContext.DataRow[0];
            Assert.AreEqual(value.Result,value.Number*value.Number);
        }

        public static IEnumerable<ExampleData> GetExampleDataList()
        {
            //sample: Result = Number*Number
            yield return new ExampleData() { Number = 0, Result = 0 };
            yield return new ExampleData() { Number = 1, Result = 1 };
            yield return new ExampleData() { Number = -1, Result = 1 };
            yield return new ExampleData() { Number = 2, Result = 4 };
        }
 * where:
 * RuntimeDataSourceProvider is the invariant name in the configuration
 * UnitTestProject1.UnitTest1 is the full class name of the data method
 * GetExampleDataList is the methodname
 * The method must return an IEnumerable.
 */
namespace RuntimeDataSourceProvider
{
    using System.Collections;
    using System.Data;
    using System.Data.Common;
    using System.Data.SqlClient;
    using System;

    public class RuntimeDataSourceProviderFactory : System.Data.Common.DbProviderFactory
    {

        public static RuntimeDataSourceProviderFactory Instance = new RuntimeDataSourceProviderFactory();

        public override DbCommand CreateCommand()
        {
            return new RuntimeDataSourceCommand();
        }

        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new RuntimeDataSourceCommandBuilder();
        }

        public override DbConnection CreateConnection()
        {
            return new RuntimeDataSourceConnection();
        }

        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new SqlConnectionStringBuilder();
        }

        public override DbDataAdapter CreateDataAdapter()
        {
            return new RuntimeDataSourceDataAdapter();
        }
    }

    public class RuntimeDataSourceConnection : DbConnection
    {
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            //Do Nothing ..
            return null;
        }

        public override void Close()
        {
            //Do Nothing ..
        }

        public override void ChangeDatabase(string databaseName)
        {
            //Do Nothing ..
        }

        public override void Open()
        {
            //Do Nothing ..
        }

        public override string ConnectionString { get; set; }

        public override string Database
        {
            get
            {
                return "RuntimeDb";
            }
        }

        public override ConnectionState State
        {
            get
            {
                return ConnectionState.Open;
            }
        }

        public override string DataSource
        {
            get
            {
                return "Runtime";
            }
        }

        public override string ServerVersion
        {
            get
            {
                return "1.0";
            }
        }

        protected override DbCommand CreateDbCommand()
        {
            return new RuntimeDataSourceCommand();
        }
    }

    public class RuntimeDataSourceCommand : DbCommand
    {

        public RuntimeDataSourceCommand()
        {

        }

        public override void Prepare()
        {
            //Do Nothing ..
        }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection { get; set; }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                return null;
            }
        }

        protected override DbTransaction DbTransaction { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override void Cancel()
        {
            //Do Nothing ..
        }

        protected override DbParameter CreateDbParameter()
        {
            //Do Nothing ..
            return null;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var commandSql = this.CommandText;
            var methodName = commandSql.Replace("select * from ", "");
            var className = this.Connection.ConnectionString;

            var classType = Type.GetType(className, true, true);
            var classInstance = Activator.CreateInstance(classType, true);

            var enumerableResult = classType.GetMethod(methodName).Invoke(classInstance, null);
            if (!(enumerableResult is IEnumerable))
            {
                throw new NotSupportedException("Invalid result from " + className + "." + methodName + ".Expected IEnumerable");
            }

            var table = new DataTable(methodName);
            table.Columns.Add("Result", typeof(object));
            foreach (var item in (IEnumerable)enumerableResult)
            {
                table.Rows.Add(new object[] { item });
            }

            return new DataTableReader(table);

        }

        public override int ExecuteNonQuery()
        {
            //Do Nothing ..
            return 1;
        }

        public override object ExecuteScalar()
        {
            //Do Nothing ..
            return 1;
        }
    }

    public class RuntimeDataSourceDataAdapter : System.Data.Common.DbDataAdapter
    {
    }

    public class RuntimeDataSourceCommandBuilder : System.Data.Common.DbCommandBuilder
    {

        public override string SchemaSeparator
        {
            get
            {
                return ".";
            }
            set
            {
            }
        }

        public override string CatalogSeparator
        {
            get
            {
                return ".";
            }
            set
            {
            }
        }
        public override string QuotePrefix
        {
            get
            {
                return " ";
            }
            set
            {
            }
        }
        public override string QuoteSuffix
        {
            get
            {
                return " ";
            }
            set
            {
            }
        }

        public override string QuoteIdentifier(string unquotedIdentifier)
        {
            return unquotedIdentifier;
        }

        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            //Do Nothing ..
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            return "";
        }

        protected override string GetParameterName(string parameterName)
        {
            return "";
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return "";
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            //Do Nothing
        }
    }
}
