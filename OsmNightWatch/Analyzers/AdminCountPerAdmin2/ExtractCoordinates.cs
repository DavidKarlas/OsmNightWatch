using NetTopologySuite.Geometries;
using OsmSharp;
using OsmSharp.Db;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2
{
    public static class ExtractCoordinates
    {
        public static IEnumerable<Coordinate> ExtractCoordinatesFromRelation(Relation relation, IOsmGeoBatchSource newOsmSource)
        {
            var result = new List<(double lat, double lon)>();
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
                    //case OsmGeoType.Relation:
                    //    foreach (var coordinate in ExtractCoordinatesFromRelation(newOsmSource.GetRelation(member.Id), newOsmSource))
                    //    {
                    //        yield return coordinate;
                    //    }
                    //    break;
                }
            }
        }
    }
}
