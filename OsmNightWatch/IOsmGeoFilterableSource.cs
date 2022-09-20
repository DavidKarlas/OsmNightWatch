using OsmSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public interface IOsmGeoFilterableSource : IOsmGeoBatchSource
    {
        IEnumerable<OsmGeo> Filter(IEnumerable<ElementFilter> filters);
    }
}
