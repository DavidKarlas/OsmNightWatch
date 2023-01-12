using OsmSharp;
using OsmSharp.Tags;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        public ElementFilter(OsmGeoType geoType, IEnumerable<TagFilter> tags)
        {
            GeoType = geoType;
            Tags = tags.ToList();
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
