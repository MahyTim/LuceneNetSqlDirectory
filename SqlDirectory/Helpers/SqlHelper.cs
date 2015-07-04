namespace SqlDirectory
{
    static class SqlHelper
    {
        public static string RemoveBrackets(string s)
        {
            return s.Replace("[", "").Replace("]", "");
        }
    }
}