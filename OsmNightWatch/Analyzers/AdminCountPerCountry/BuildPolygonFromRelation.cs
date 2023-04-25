using NetTopologySuite.Geometries;
using NetTopologySuite.Operation.Polygonize;
using NetTopologySuite.Operation.Union;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public static class BuildPolygonFromRelation
    {
        public static (Geometry Polygon, List<Way> Ways) BuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            var result = InternalBuildPolygon(relation, newOsmSource);
            if (result.Polygon == MultiPolygon.Empty)
                return result;
            try
            {
                if (result.Polygon.Geometries.Length == 1)
                {
                    return (result.Polygon.Geometries[0], result.Ways);
                }
                var unionizedPolygon = CascadedPolygonUnion.Union(result.Polygon.Geometries);
                return (unionizedPolygon, result.Ways);
            }
            catch
            {
                return result;
            }
        }

        private static (MultiPolygon Polygon, List<Way> Ways) InternalBuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            var innerWays = new List<Way>();
            var outerWays = new List<Way>();

            foreach (var member in relation.Members)
            {
                switch (member.Role)
                {
                    case "inner":
                        if (member.Type == OsmGeoType.Way)
                            innerWays.Add(newOsmSource.GetWay(member.Id));
                        break;
                    case "outer":
                        if (member.Type == OsmGeoType.Way)
                            outerWays.Add(newOsmSource.GetWay(member.Id));
                        break;
                }
            }
            try
            {
                var outerPolygonizer = new Polygonizer();
                foreach (var outerWay in outerWays)
                {
                    var array = new Coordinate[outerWay.Nodes.Length];
                    for (int i = 0; i < outerWay.Nodes.Length; i++)
                    {
                        var node = newOsmSource.GetNode(outerWay.Nodes[i]);
                        array[i] = node.ToCoordinate();
                    }
                    outerPolygonizer.Add(new LineString(array.ToArray()));
                }
                var outerPolygons = outerPolygonizer.GetPolygons().OfType<Polygon>().ToArray();

                if (innerWays.Count > 0)
                {
                    Polygonizer innerPolygonizer = new Polygonizer();
                    foreach (var innerWay in innerWays)
                    {
                        innerPolygonizer.Add(new LineString(innerWay.Nodes.Select(x => newOsmSource.GetNode(x).ToCoordinate()).ToArray()));
                    }

                    for (int i = 0; i < outerPolygons.Length; i++)
                    {
                        var poly = outerPolygons[i];
                        var inners = new List<LinearRing>();
                        foreach (var innerPolygon in innerPolygonizer.GetPolygons().OfType<Polygon>())
                        {
                            if (poly.Contains(innerPolygon))
                            {
                                inners.Add(innerPolygon.Shell);
                            }
                        }
                        outerPolygons[i] = new Polygon(poly.Shell, inners.ToArray());
                    }
                }
                outerWays.AddRange(innerWays);
                return (new MultiPolygon(outerPolygons), outerWays);
            }
            catch (Exception)
            {
                //TODO, add exception message to Issue description
            }
            outerWays.AddRange(innerWays);
            return (MultiPolygon.Empty, outerWays);
        }
    }
}