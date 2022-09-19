using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
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
