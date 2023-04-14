using OsmNightWatch.Lib;
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
        IEnumerable<OsmGeo> Filter(FilterSettings filterSettings);
    }

    public interface IOsmValidateSource : IOsmGeoFilterableSource
    {
        IEnumerable<IssueData> Validate(Func<OsmGeo, IssueData?> validator, FilterSettings filterSettings);
    }
}
