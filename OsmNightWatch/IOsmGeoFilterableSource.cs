using OsmNightWatch.Lib;
using OsmNightWatch.PbfParsing;

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
