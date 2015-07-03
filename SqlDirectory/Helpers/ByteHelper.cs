using System;
using System.Collections.Generic;

namespace SqlDirectory
{
    static class ByteHelper
    {
        public static byte[] Combine(byte[] first, byte[] second)
        {
            byte[] ret = new byte[first.Length + second.Length];
            Buffer.BlockCopy(first, 0, ret, 0, first.Length);
            Buffer.BlockCopy(second, 0, ret, first.Length, second.Length);
            return ret;
        }
    }

    public class ByteWriter
    {
        public void Add(long position, byte[] array)
        {
            
        }

        public IEnumerable<Segment> GetSegments()
        {
            return null;
        }
    }

    public class Segment
    {
        public int Position { get; set; }
        public byte[] Buffer { get; set; }
    }
}