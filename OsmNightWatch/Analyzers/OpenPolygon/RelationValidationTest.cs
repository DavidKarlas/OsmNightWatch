using OsmSharp;
using OsmSharp.Db;

namespace OsmNightWatch.Analyzers.OpenPolygon
{
    internal class RelationValidationTest
    {
        public bool Visit(Relation r, IOsmGeoSource db)
        {
            var hashset = new HashSet<long>();

            foreach (var way in r.Members.Where(m => (m.Role == "inner" || m.Role == "outer") && m.Type == OsmGeoType.Way).Select(m => db.GetWay(m.Id)))
            {
                var nodes = way.Nodes;
                long first = nodes[0];
                long last = nodes[nodes.Length - 1];

                if (!hashset.Remove(first))
                {
                    hashset.Add(first);
                }
                if (!hashset.Remove(last))
                {
                    hashset.Add(last);
                }
            }

            if (hashset.Count > 0)
            {
                return true;
            }
            return false;
        }
    }
}
