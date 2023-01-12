using NetTopologySuite.Geometries;
using NetTopologySuite.Geometries.Prepared;

namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2;

class ProcessingCountry
{
    public ProcessingCountry(Country country, MultiPolygon polygon)
    {
        Country = country;
        Polygon = new PreparedPolygon(polygon);
    }

    public Country Country { get; }

    public PreparedPolygon Polygon { get; }
}