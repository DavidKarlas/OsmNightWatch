using OsmNightWatch.PbfParsing;
using OsmSharp.Changesets;
using System.Xml.Linq;

namespace OsmNightWatch
{
    public class MergedChangeset
    {
        public OsmChange OriginalChangeset { get; }

        public Dictionary<long, Relation?> Relations { get; }
        public Dictionary<long, Way?> Ways { get; }
        public Dictionary<long, Node?> Nodes { get; }

        public MergedChangeset(OsmChange changeset)
        {
            OriginalChangeset = changeset;

            var nodes = new Dictionary<long, OsmSharp.Node>();
            var ways = new Dictionary<long, OsmSharp.Way>();
            var relations = new Dictionary<long, OsmSharp.Relation>();

            foreach (var element in changeset.Create)
            {
                switch (element.Type)
                {
                    case OsmSharp.OsmGeoType.Node:
                        nodes[(long)element.Id!] = (OsmSharp.Node)element;
                        break;
                    case OsmSharp.OsmGeoType.Way:
                        ways[(long)element.Id!] = (OsmSharp.Way)element;
                        break;
                    case OsmSharp.OsmGeoType.Relation:
                        relations[(long)element.Id!] = (OsmSharp.Relation)element;
                        break;
                }
            }
            foreach (var element in changeset.Modify)
            {
                switch (element.Type)
                {
                    case OsmSharp.OsmGeoType.Node:
                        nodes[(long)element.Id!] = (OsmSharp.Node)element;
                        break;
                    case OsmSharp.OsmGeoType.Way:
                        ways[(long)element.Id!] = (OsmSharp.Way)element;
                        break;
                    case OsmSharp.OsmGeoType.Relation:
                        relations[(long)element.Id!] = (OsmSharp.Relation)element;
                        break;
                }
            }
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmSharp.OsmGeoType.Node:
                        if (change.Id is long idNode)
                        {
                            if (nodes.TryGetValue(idNode, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            nodes[idNode] = null;
                        }
                        break;
                    case OsmSharp.OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            if (ways.TryGetValue(idWay, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            ways[idWay] = null;
                        }
                        break;
                    case OsmSharp.OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                        {
                            if (relations.TryGetValue(idRelation, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            relations[idRelation] = null;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }

            Nodes = nodes.ToDictionary(x => x.Key, x => x.Value == null ? null : new Node(x.Key, (double)x.Value.Latitude!, (double)x.Value.Longitude!, x.Value.Tags));
            Ways = ways.ToDictionary(x => x.Key, x => x.Value == null ? null : new Way(x.Key, x.Value.Nodes, x.Value.Tags));
            Relations = relations.ToDictionary(x => x.Key, x => x.Value == null ? null : new Relation(x.Key, x.Value.Members.Select(m => new RelationMember(m.Id, m.Role, (OsmGeoType)m.Type)).ToArray(), x.Value.Tags));
        }
    }
}
