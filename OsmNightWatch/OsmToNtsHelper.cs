using NetTopologySuite.Geometries;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch
{
    public static class OsmToNtsHelper
    {
        public static Coordinate ToCoordinate(this Node node)
        {
            return new Coordinate((double)node.Longitude!, (double)node.Latitude!);
        }
    }
}
