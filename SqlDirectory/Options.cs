namespace SqlDirectory
{
    public class Options
    {
        public string SchemaName { get; set; }
        public int LockTimeoutInMinutes { get; set; }

        public Options()
        {
            SchemaName = "[dbo]";
            LockTimeoutInMinutes = 10;
        }
    }
}