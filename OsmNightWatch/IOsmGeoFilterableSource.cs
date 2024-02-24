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
        void Validate(Action<OsmGeo> validator, FilterSettings filterSettings);
    }
}
