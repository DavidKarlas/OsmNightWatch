using NetTopologySuite.Geometries;
using OsmSharp.Complete;
using OsmSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OsmSharp.Db;
using NetTopologySuite.Operation.Polygonize;
using NetTopologySuite.Operation.Union;

namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2
{
    public static class BuildPolygonFromRelation
    {
        class MyRing
        {
            public long firstNode;
            public long lastNode;
            public List<Way> Ways = new();

            public MyRing(Way way)
            {
                firstNode = way.Nodes.First();
                lastNode = way.Nodes.Last();
                Ways.Add(way);
            }

            public bool FastMerge(Way way)
            {
                var nodes = way.Nodes;
                long first = nodes[0];
                long last = nodes[nodes.Length - 1];

                if (firstNode == first)
                {
                    firstNode = last;
                    Ways.Insert(0, way);
                    return true;
                }
                else if (firstNode == last)
                {
                    firstNode = first;
                    Ways.Insert(0, way);
                    return true;
                }
                else if (lastNode == first)
                {
                    lastNode = last;
                    Ways.Add(way);
                    return true;
                }
                else if (lastNode == last)
                {
                    lastNode = first;
                    Ways.Add(way);
                    return true;
                }
                else
                    return false;
            }

            //public bool FastMerge(MyRing graph)
            //{
            //    long first = graph.firstNode;
            //    long last = graph.lastNode;

            //    if (firstNode == first)
            //    {
            //        firstNode = last;
            //        Ways.InsertRange(0, graph.Ways);
            //        return true;
            //    }
            //    else if (firstNode == last)
            //    {
            //        firstNode = first;
            //        Ways.InsertRange(0, graph.Ways);
            //        return true;
            //    }
            //    else if (lastNode == first)
            //    {
            //        lastNode = last;
            //        Ways.AddRange(graph.Ways);
            //        return true;
            //    }
            //    else if (lastNode == last)
            //    {
            //        lastNode = first;
            //        Ways.AddRange(graph.Ways);
            //        return true;
            //    }
            //    else
            //        return false;
            //}
        }
        public static MultiPolygon BuildPolygon(Relation relation, IOsmGeoBatchSource newOsmSource)
        {
            var result = InternalBuildPolygon(relation, newOsmSource, new HashSet<long>());
            if (result == MultiPolygon.Empty)
                return MultiPolygon.Empty;
            var result2 = CascadedPolygonUnion.Union(result.Geometries);
            if (result2 is MultiPolygon mp)
                return mp;
            return new MultiPolygon(new[] { (Polygon)result2 });
        }

        private static MultiPolygon InternalBuildPolygon(Relation relation, IOsmGeoBatchSource newOsmSource, HashSet<long> visitedSubareas)
        {
            if (!visitedSubareas.Add((long)relation.Id!))
            {
                Console.WriteLine("Subarea cycle found.");//TODO: Report as issue on NightWatch
                return MultiPolygon.Empty;
            }
            if (!OpenPolygon.AdminOpenPolygonAnalyzer.IsValid(relation, newOsmSource, false))
                return MultiPolygon.Empty;

            var innerWays = new List<Way>();
            var outerWays = new List<Way>();
            var subareas = new List<Polygon>();
            foreach (var member in relation.Members)
            {
                switch (member.Role)
                {
                    case "inner":
                        if (member.Type != OsmGeoType.Way)
                            throw new NotImplementedException();
                        innerWays.Add(newOsmSource.GetWay(member.Id));
                        break;
                    case "outer":
                        switch (member.Type)
                        {
                            case OsmGeoType.Way:
                                outerWays.Add(newOsmSource.GetWay(member.Id));
                                break;
                            case OsmGeoType.Relation:
                                subareas.AddRange(InternalBuildPolygon(newOsmSource.GetRelation(member.Id), newOsmSource, visitedSubareas).Geometries.OfType<Polygon>());
                                break;
                        }
                        break;
                }
            }

            if (outerWays.Count == 0)
            {
                foreach (var member in relation.Members)
                {
                    switch (member.Role)
                    {
                        case "subarea":
                            switch (member.Type)
                            {
                                case OsmGeoType.Relation:
                                    subareas.AddRange(InternalBuildPolygon(newOsmSource.GetRelation(member.Id), newOsmSource, visitedSubareas).Geometries.OfType<Polygon>());
                                    break;
                                case OsmGeoType.Way:
                                    try
                                    {
                                        subareas.Add(new Polygon(new LinearRing(newOsmSource.GetWay(member.Id).Nodes.Select(node => newOsmSource.GetNode(node).ToCoordinate()).ToArray())));
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"Way {member.Id} is invalid subarea");
                                    }
                                    break;
                            }
                            break;
                    }
                }
            }

            Polygonizer outerPolygonizer = new Polygonizer();
            foreach (var outerWay in outerWays)
            {
                outerPolygonizer.Add(new LineString(outerWay.Nodes.Select(x => newOsmSource.GetNode(x).ToCoordinate()).ToArray()));
            }
            var outerPolygons = outerPolygonizer.GetPolygons().OfType<Polygon>().ToArray();

            if (innerWays.Count > 0)
            {
                //TODO: handle inner ways
                //Polygonizer innerPolygonizer = new Polygonizer();
                //foreach (var innerWay in innerWays)
                //{
                //    innerPolygonizer.Add(new LineString(innerWay.Nodes.Select(x => newOsmSource.GetNode(x).ToCoordinate()).ToArray()));
                //}

                //foreach (var innerPolygon in innerPolygonizer.GetPolygons())
                //{

                //}
            }

            if (subareas.Count > 0)
            {
                outerPolygons = outerPolygons.Concat(subareas).ToArray();
            }

            return new MultiPolygon(outerPolygons);
        }

        //private static List<MyRing> BuildRings(List<Way> inputWays)
        //{
        //    var startNodes = new Dictionary<long, MyRing>();
        //    var endNodes = new Dictionary<long, MyRing>();
        //    var completeRings = new List<MyRing>();
        //    foreach (var way in inputWays)
        //    {
        //        long first = way.Nodes[0];
        //        long last = way.Nodes[way.Nodes.Length - 1];
        //        if (startNodes.TryGetValue(first, out var ring))
        //        {
        //            startNodes.Remove(first);
        //            if (last == ring.lastNode)
        //            {
        //                endNodes.Remove(last);
        //                completeRings.Add(ring);
        //            }
        //            else
        //            {
        //                startNodes.Add()
        //            }
        //        }
        //        else if (endNodes.TryGetValue(first, out ring))
        //        {
        //            ring.FastMerge(way);
        //        }
        //        else if (startNodes.TryGetValue(last, out ring))
        //        {
        //            ring.FastMerge(way);
        //        }
        //        else if (endNodes.TryGetValue(last, out ring))
        //        {
        //            ring.FastMerge(way);
        //        }
        //        else
        //        {
        //            ring = new MyRing(way);
        //            startNodes.Add(ring.firstNode, ring);
        //            endNodes.Add(ring.lastNode, ring);
        //        }
        //    }
        //    var completeGraphs = graphs.Where(g => g.firstNode == g.lastNode).ToList();
        //    foreach (var completeGraph in completeGraphs)
        //    {
        //        graphs.Add(completeGraph);
        //    }
        //    bool mergedSomething = graphs.Count > 0;
        //    while (mergedSomething)
        //    {
        //        mergedSomething = false;
        //        var newGraphs = new List<MyRing>();
        //        foreach (MyRing graph in graphs)
        //        {
        //            foreach (MyRing graph2 in graphs)
        //            {
        //                if (graph == graph2)
        //                    continue;
        //                if (graph.FastMerge(graph2))
        //                {
        //                    mergedSomething = true;
        //                    graphs.Remove(graph2);
        //                }
        //            }
        //        }
        //    }
        //    if (graphs.Count > 1)
        //    {
        //        throw new InvalidOperationException();// this shouldn't happen...
        //    }
        //    return graphs;
        //}
    }
}