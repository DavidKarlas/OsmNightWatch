using OsmSharp;
using OsmSharp.Tags;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public class ElementFilter
    {
        public OsmGeoType GeoType { get; }
        
        public List<TagFilter> Tags { get; }

        public ElementFilter(OsmGeoType geoType, IEnumerable<TagFilter> tags)
        {
            GeoType = geoType;
            Tags = tags.ToList();
        }

        public bool Matches(OsmGeo element)
        {
            if (element.Type != GeoType)
                throw new InvalidOperationException();
            foreach (var tagFilter in Tags)
            {
                if (element.Tags == null || !tagFilter.Matches(element.Tags))
                    return false;
            }
            return true;
        }
    }

    public class TagFilter
    {
        public string KeyFilter { get; }
        public string? ValueFilter { get; }

        public TagFilter(string keyFilter, string? valueFilter = null)
        {
            KeyFilter = keyFilter;
            ValueFilter = valueFilter;
        }

        public bool Matches(TagsCollectionBase tags)
        {
            if (tags.TryGetValue(KeyFilter, out var value))
            {
                if (ValueFilter == null)
                    return true;
                return value == ValueFilter;
            }
            return false;
        }
    }
}
