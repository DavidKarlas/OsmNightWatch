using NetTopologySuite.Geometries;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public static class ExtractCoordinates
    {
        public static IEnumerable<Coordinate> ExtractCoordinatesFromRelation(Relation relation, IOsmGeoBatchSource newOsmSource)
        {
            foreach (var member in relation.Members)
            {
                switch (member.Type)
                {
                    case OsmGeoType.Node:
                        yield return newOsmSource.GetNode(member.Id).ToCoordinate();
                        break;
                    case OsmGeoType.Way:
                        foreach (var node in newOsmSource.GetWay(member.Id).Nodes)
                        {
                            yield return newOsmSource.GetNode(node).ToCoordinate();
                        }
                        break;
                }
            }
        }
    }
}
