using NetTopologySuite.Geometries;
using NetTopologySuite.Operation.Polygonize;
using NetTopologySuite.Operation.Union;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public static class BuildPolygonFromRelation
    {
        public static (Geometry Polygon, List<Way> Ways, string? reason) BuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            var result = InternalBuildPolygon(relation, newOsmSource);
            if (result.Polygon == MultiPolygon.Empty)
                return result;
            try
            {
                if (result.Polygon.Geometries.Length == 1)
                {
                    return (result.Polygon.Geometries[0], result.Ways, null);
                }
                var unionizedPolygon = CascadedPolygonUnion.Union(result.Polygon.Geometries);
                return (unionizedPolygon, result.Ways, null);
            }
            catch
            {
                return result;
            }
        }

        private static (MultiPolygon Polygon, List<Way> Ways, string? reason) InternalBuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            var innerWays = new List<Way>();
            var outerWays = new List<Way>();
            bool atLeastOneWay = false;
            foreach (var member in relation.Members)
            {
                switch (member.Role)
                {
                    case "inner":
                        if (member.Type == OsmGeoType.Way)
                        {
                            atLeastOneWay = true;
                            innerWays.Add(newOsmSource.GetWay(member.Id));
                        }
                        break;
                    case "outer":
                        if (member.Type == OsmGeoType.Way)
                        {
                            atLeastOneWay = true;
                            outerWays.Add(newOsmSource.GetWay(member.Id));
                        }
                        break;
                    case "":
                        return (MultiPolygon.Empty, new List<Way>(0), "Member without role");
                }
            }

            if (!atLeastOneWay)
            {
                return (MultiPolygon.Empty, new List<Way>(0), "Missing ways");
            }

            if (outerWays.Count == 0)
            {
                return (MultiPolygon.Empty, new List<Way>(0), "No outer ways found");
            }
            try
            {
                var outerPolygonizer = new PolygonizeGraph(GeometryFactory.Default);
                foreach (var outerWay in outerWays)
                {
                    var array = new Coordinate[outerWay.Nodes.Length];
                    for (int i = 0; i < outerWay.Nodes.Length; i++)
                    {
                        var node = newOsmSource.GetNode(outerWay.Nodes[i]);
                        array[i] = node.ToCoordinate();
                    }
                    outerPolygonizer.AddEdge(new LineString(array.ToArray()));
                }

                if (outerPolygonizer.DeleteDangles().Any())
                {
                    return (MultiPolygon.Empty, outerWays, "Some outer ways are unused.");
                }
                if (outerPolygonizer.DeleteCutEdges().Any())
                {
                    return (MultiPolygon.Empty, outerWays, "Some outer ways form cut edges.");
                }
                var outerRings = outerPolygonizer.GetEdgeRings().ToArray();
                if (outerRings.Any(edge => !edge.IsValid))
                {
                    return (MultiPolygon.Empty, outerWays, "Some outer ways form invalid rings.");
                }

                var outerPolygons = outerRings.Where(er => { er.ComputeHole(); return !er.IsHole; }).Select(edge => edge.Polygon).ToArray();
                if (outerPolygons.Any(p => !p.IsValid))
                {
                    return (MultiPolygon.Empty, outerWays, "Invalid polygon.");
                }
                if (outerPolygons.Length == 0)
                {
                    return (MultiPolygon.Empty, outerWays, "No valid polygon found.");
                }


                if (innerWays.Count > 0)
                {
                    Polygonizer innerPolygonizer = new Polygonizer();
                    foreach (var innerWay in innerWays)
                    {
                        innerPolygonizer.Add(new LineString(innerWay.Nodes.Select(x => newOsmSource.GetNode(x).ToCoordinate()).ToArray()));
                    }

                    if (innerPolygonizer.GetInvalidRingLines().Count > 0)
                    {
                        return (MultiPolygon.Empty, outerWays, "Inner ways have invalid ring lines.");
                    }

                    if (innerPolygonizer.GetDangles().Count > 0)
                    {
                        return (MultiPolygon.Empty, outerWays, "Inner ways have unused sections.");
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
                return (new MultiPolygon(outerPolygons), outerWays, null);
            }
            catch (Exception ex)
            {
                outerWays.AddRange(innerWays);
                return (MultiPolygon.Empty, outerWays, ex.Message);
            }
        }
    }
}