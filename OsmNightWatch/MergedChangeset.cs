using OsmSharp;
using OsmSharp.Changesets;

namespace OsmNightWatch
{
    public class MergedChangeset
    {
        public OsmChange OriginalChangeset { get; }

        public Dictionary<uint, PbfParsing.Relation?> Relations { get; }
        public Dictionary<uint, PbfParsing.Way?> Ways { get; }
        public Dictionary<long, PbfParsing.Node?> Nodes { get; }

        public Dictionary<long, Relation?> OsmRelations { get; }
        public Dictionary<long, Way?> OsmWays { get; }
        public Dictionary<long, Node?> OsmNodes { get; }

        public MergedChangeset(OsmChange changeset)
        {
            OriginalChangeset = changeset;

            var nodes = new Dictionary<long, Node?>();
            var ways = new Dictionary<long, Way?>();
            var relations = new Dictionary<long, Relation?>();

            foreach (var element in changeset.Create)
            {
                switch (element.Type)
                {
                    case OsmGeoType.Node:
                        nodes[(long)element.Id!] = (Node)element;
                        break;
                    case OsmGeoType.Way:
                        ways[(long)element.Id!] = (Way)element;
                        break;
                    case OsmGeoType.Relation:
                        relations[(long)element.Id!] = (Relation)element;
                        break;
                }
            }
            foreach (var element in changeset.Modify)
            {
                switch (element.Type)
                {
                    case OsmGeoType.Node:
                        nodes[(long)element.Id!] = (Node)element;
                        break;
                    case OsmGeoType.Way:
                        ways[(long)element.Id!] = (Way)element;
                        break;
                    case OsmGeoType.Relation:
                        relations[(long)element.Id!] = (Relation)element;
                        break;
                }
            }
            foreach (var change in changeset.Delete)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                        {
                            if (nodes.TryGetValue(idNode, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            nodes[idNode] = null;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            if (ways.TryGetValue(idWay, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            ways[idWay] = null;
                        }
                        break;
                    case OsmGeoType.Relation:
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

            OsmNodes = nodes;
            OsmWays = ways;
            OsmRelations = relations;

            Nodes = nodes.ToDictionary(x => x.Key, x => x.Value == null ? null : new PbfParsing.Node(x.Key, (double)x.Value.Latitude!, (double)x.Value.Longitude!, x.Value.Tags));
            Ways = ways.ToDictionary(x => (uint)x.Key, x => x.Value == null ? null : new PbfParsing.Way(x.Key, x.Value.Nodes, x.Value.Tags));
            Relations = relations.ToDictionary(x => (uint)x.Key, x => x.Value == null ? null : new PbfParsing.Relation(x.Key, x.Value.Members.Select(m => new PbfParsing.RelationMember(m.Id, m.Role, (PbfParsing.OsmGeoType)m.Type)).ToArray(), x.Value.Tags));
        }
    }
}
