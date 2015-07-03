using System.Data;
using System.Data.SqlClient;

namespace SqlDirectory
{
    static class BlobHelper
    {
        public static void ReadBytes(this SqlConnection connection, string name, long position, byte[] b, int offset, int len, string schemaName)
        {
            if (b.Length == 0)
                return;
            using (var command = new SqlCommand($"SELECT Content FROM {schemaName}.[FileContents] WHERE Name = @name", connection))
            {
                command.Parameters.AddWithValue("name", name);
                using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
                {
                    if (reader.HasRows)
                    {
                        reader.Read();
                        if (false == reader.IsDBNull(0))
                        {
                            reader.GetBytes(0, position, b, offset, len);
                        }
                    }
                }
            }
        }
        public static void Write(this SqlConnection connection, byte[] buffer, int index, int len, string name, bool isFirstWrite, string schemaName)
        {
            if (isFirstWrite)
            {
                using (var cmdFirstChunk = new SqlCommand($"UPDATE {schemaName}.[FileContents] SET [Content] = @firstChunk WHERE [Name] = @name", connection))
                {
                    cmdFirstChunk.Parameters.AddWithValue("name", name);
                    var paramChunk = new SqlParameter("@firstChunk", SqlDbType.VarBinary, -1);
                    cmdFirstChunk.Parameters.Add(paramChunk);
                    paramChunk.Value = buffer;
                    cmdFirstChunk.ExecuteNonQuery();
                }
            }
            using (var cmdAppendChunk = new SqlCommand($"UPDATE {schemaName}.[FileContents] SET [Content].WRITE(@chunk, @index, @len) WHERE [Name] = @name", connection))
            {
                cmdAppendChunk.Parameters.AddWithValue("@name", name);
                var paramChunk = new SqlParameter("@chunk", SqlDbType.VarBinary, -1);
                cmdAppendChunk.Parameters.Add(paramChunk);
                paramChunk.Value = buffer;
                cmdAppendChunk.Parameters.AddWithValue("index", index);
                cmdAppendChunk.Parameters.AddWithValue("len", len);
                cmdAppendChunk.ExecuteNonQuery();
            }
        }
    }
}