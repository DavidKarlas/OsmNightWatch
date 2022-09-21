using OsmSharp;
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
    }
}
