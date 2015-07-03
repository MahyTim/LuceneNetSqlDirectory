namespace SqlDirectory
{
    public class Options
    {
        public string SchemaName { get; set; }

        public Options()
        {
            SchemaName = "[dbo]";
        }
    }
}