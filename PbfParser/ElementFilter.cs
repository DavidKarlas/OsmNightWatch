using OsmNightWatch.PbfParsing;
using OsmSharp.Tags;

namespace OsmNightWatch
{
    public class FilterSettings
    {
        public List<ElementFilter> Filters { get; init; } = new List<ElementFilter>();
    }

    public class ElementFilter
    {
        public OsmGeoType GeoType { get; }
        
        public List<TagFilter> Tags { get; }

        public bool NeedsWays { get; }
        
        public bool NeedsNodes { get; }

        public ElementFilter(OsmGeoType geoType, IEnumerable<TagFilter> tags, bool needsWays = false, bool needsNodes = false) {
            GeoType = geoType;
            Tags = tags.ToList();
            NeedsWays = needsWays;
            NeedsNodes = needsNodes;
        }

        public bool Matches(OsmGeo element)
        {
            if (element.Type != GeoType)
                throw new InvalidOperationException();
            if (element.Tags == null)
                return false;
            foreach (var tagFilter in Tags)
            {
                if (!tagFilter.Matches(element.Tags))
                    return false;
            }
            return true;
        }
    }

    public class TagFilter
    {
        public string KeyFilter { get; }
        public HashSet<string> ValidValues { get; }

        public TagFilter(string keyFilter, params string[] validValues)
        {
            KeyFilter = keyFilter;
            ValidValues = new HashSet<string>(validValues);
        }

        public bool Matches(TagsCollectionBase tags)
        {
            if (tags.TryGetValue(KeyFilter, out var value))
            {
                if (ValidValues.Count == 0)
                    return true;
                return ValidValues.Contains(value);
            }
            return false;
        }
    }
}
