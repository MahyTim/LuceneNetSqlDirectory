namespace LuceneNetSqlDirectory
{
    public class Options
    {
        /// <summary>
        /// The database schema name in which the structure is provisioned. By default this is 'dbo'.
        /// </summary>
        public string SchemaName { get; set; }
        /// <summary>
        /// Locks are automatically released after a certain time window, by default this is 10 minutes. 
        /// If you need to do batch style jobs for adding/deleting/updating documents, increase this setting!
        /// </summary>
        public int LockTimeoutInMinutes { get; set; }

        public Options()
        {
            SchemaName = "[dbo]";
            LockTimeoutInMinutes = 10;
        }
    }
}