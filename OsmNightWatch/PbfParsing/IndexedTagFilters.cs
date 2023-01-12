using System.Text;

namespace OsmNightWatch.PbfParsing
{
    public class IndexedTagFilters
    {
        public readonly List<(Memory<byte> TagKey, List<Memory<byte>> TagValues)> Utf8TagsFilter;
        public Dictionary<int, List<Memory<byte>>> StringLengths = new();

        public IndexedTagFilters(IEnumerable<TagFilter> tagFilters)
        {
            Utf8TagsFilter = new();
            foreach (var filterGroup in tagFilters.GroupBy(tf => tf.KeyFilter))
            {
                Utf8TagsFilter.Add(
                    (
                        Encoding.UTF8.GetBytes(filterGroup.Key),
                        filterGroup.Where(g => g.ValidValues.Count != 0)
                        .SelectMany(g => g.ValidValues)
                        .Select(val => (Memory<byte>)Encoding.UTF8.GetBytes(val))
                        .ToList())
                    );
            }
            foreach (var filter in Utf8TagsFilter)
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
