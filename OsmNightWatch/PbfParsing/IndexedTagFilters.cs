using System.Text;

namespace OsmNightWatch.PbfParsing
{
    public class IndexedTagFilters
    {
        public readonly List<(Memory<byte> TagKey, List<Memory<byte>> TagValues)> Utf8RelationsTagsFilter;
        public Dictionary<int, List<Memory<byte>>> StringLengths = new();

        public IndexedTagFilters(List<(string TagKey, string TagValue)> tagFilters)
        {
            Utf8RelationsTagsFilter = new();
            foreach (var filterGroup in tagFilters.GroupBy(tf => tf.TagKey))
            {
                Utf8RelationsTagsFilter.Add(
                    (
                        Encoding.UTF8.GetBytes(filterGroup.Key),
                        filterGroup.Where(g => !string.IsNullOrEmpty(g.TagValue))
                            .Select(g => (Memory<byte>)Encoding.UTF8.GetBytes(g.TagValue)).ToList())
                    );
            }
            foreach (var filter in Utf8RelationsTagsFilter)
            {
                InsertNewString(filter.TagKey);
                foreach (var tagValue in filter.TagValues)
                {
                    InsertNewString(tagValue);
                }
            }

            void InsertNewString(Memory<byte> utf8String)
            {
                if (!StringLengths.TryGetValue(utf8String.Length, out var list))
                {
                    StringLengths[utf8String.Length] = list = new List<Memory<byte>>();
                }
                list.Add(utf8String);
            }
        }
    }
}
