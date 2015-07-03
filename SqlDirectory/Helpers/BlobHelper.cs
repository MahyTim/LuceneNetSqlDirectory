using System;
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

        public static void Write(this SqlConnection connection, byte[] buffer, int index, int count, string name, bool isFirstWrite, string schemaName)
        {
            using (var cmdFirstChunk = new SqlCommand($"UPDATE {schemaName}.[FileContents] SET [Content] = @firstChunk WHERE [Name] = @name", connection))
            {
                cmdFirstChunk.Parameters.AddWithValue("@name", name);
                using (var cmdAppendChunk = new SqlCommand($"UPDATE {schemaName}.[FileContents] SET [Content].WRITE(@chunk, @index, @count) WHERE [Name] = @name", connection))
                {
                    cmdAppendChunk.Parameters.AddWithValue("@name", name);

                    var paramChunk = new SqlParameter("@chunk", SqlDbType.VarBinary, -1);
                    cmdAppendChunk.Parameters.Add(paramChunk);

                    byte[] bytesToWrite = buffer;
                    if (isFirstWrite)
                    {
                        cmdFirstChunk.Parameters.AddWithValue("@firstChunk", buffer);
                        cmdFirstChunk.ExecuteNonQuery();
                    }
                    {
                        paramChunk.Value = bytesToWrite;
                        cmdAppendChunk.Parameters.AddWithValue("index", index);
                        cmdAppendChunk.Parameters.AddWithValue("count", count);
                        cmdAppendChunk.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}