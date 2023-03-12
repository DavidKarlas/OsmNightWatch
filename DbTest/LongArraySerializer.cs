// See https://aka.ms/new-console-template for more information
using Tenray.ZoneTree.Serializers;

internal class LongArraySerializer : ISerializer<long[]>
{
    public long[] Deserialize(byte[] bytes)
    {
        var result = new long[bytes.Length / sizeof(long)];
        Buffer.BlockCopy(bytes, 0, result, 0, bytes.Length);
        return result;
    }

    public byte[] Serialize(in long[] entry)
    {
        var result = new byte[entry.Length * sizeof(long)];
        Buffer.BlockCopy(entry, 0, result, 0, result.Length);
        return result;
    }
}