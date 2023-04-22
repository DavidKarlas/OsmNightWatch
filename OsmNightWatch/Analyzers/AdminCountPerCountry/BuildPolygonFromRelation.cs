using NetTopologySuite.Geometries;
using NetTopologySuite.Operation.Polygonize;
using NetTopologySuite.Operation.Union;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry
{
    public static class BuildPolygonFromRelation
    {
        public static (MultiPolygon Polygon, List<Way> Ways) BuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            var result = InternalBuildPolygon(relation, newOsmSource);
            if (result.Polygon == MultiPolygon.Empty)
                return (MultiPolygon.Empty, result.Ways);
            var result2 = CascadedPolygonUnion.Union(result.Polygon.Geometries);
            if (result2 is MultiPolygon mp)
                return (mp, result.Ways);
            return (new MultiPolygon(new[] { (Polygon)result2 }), result.Ways);
        }

        private static (MultiPolygon Polygon, List<Way> Ways) InternalBuildPolygon(Relation relation, IOsmGeoSource newOsmSource)
        {
            //var innerWays = new List<Way>();
            var outerWays = new List<Way>();

            if (!OpenPolygon.AdminOpenPolygonAnalyzer.IsValid(relation, newOsmSource, false))
                return (MultiPolygon.Empty, outerWays);

            foreach (var member in relation.Members)
            {
                switch (member.Role)
                {
                    //case "inner":
                    //    if (member.Type != OsmGeoType.Way)
                    //        throw new NotImplementedException();
                    //    innerWays.Add(newOsmSource.GetWay(member.Id));
                    //    break;
                    case "outer":
                        switch (member.Type)
                        {
                            case OsmGeoType.Way:
                                outerWays.Add(newOsmSource.GetWay(member.Id));
                                break;
                        }
                        break;
                }
            }

            Polygonizer outerPolygonizer = new Polygonizer();
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

            //TODO: handle inner ways
            //if (innerWays.Count > 0)
            //{
            //    //Polygonizer innerPolygonizer = new Polygonizer();
            //    //foreach (var innerWay in innerWays)
            //    //{
            //    //    innerPolygonizer.Add(new LineString(innerWay.Nodes.Select(x => newOsmSource.GetNode(x).ToCoordinate()).ToArray()));
            //    //}

            //    //foreach (var innerPolygon in innerPolygonizer.GetPolygons())
            //    //{

            //    //}
            //}

            //TODO: include inner ways when used
            return (new MultiPolygon(outerPolygons), outerWays);
        }
    }
}