using OsmSharp;
using OsmSharp.Changesets;

namespace OsmNightWatch
{
    public class MergedChangeset
    {
        public Dictionary<uint, PbfParsing.Relation?> Relations { get; private set; }
        public Dictionary<uint, PbfParsing.Way?> Ways { get; private set; }
        public Dictionary<long, PbfParsing.Node?> Nodes { get; private set; }

        public Dictionary<long, Relation?> OsmRelations { get; } = new();
        public Dictionary<long, Way?> OsmWays { get; } = new();
        public Dictionary<long, Node?> OsmNodes { get; } = new();

        public void Add(OsmChange changeset)
        {
            foreach (var element in changeset.Create)
            {
                switch (element.Type)
                {
                    case OsmGeoType.Node:
                        OsmNodes[(long)element.Id!] = (Node)element;
                        break;
                    case OsmGeoType.Way:
                        OsmWays[(long)element.Id!] = (Way)element;
                        break;
                    case OsmGeoType.Relation:
                        OsmRelations[(long)element.Id!] = (Relation)element;
                        break;
                }
            }
            foreach (var change in changeset.Modify)
            {
                switch (change.Type)
                {
                    case OsmGeoType.Node:
                        if (change.Id is long idNode)
                        {
                            if (OsmNodes.TryGetValue(idNode, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmNodes[(long)change.Id!] = (Node)change;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            if (OsmWays.TryGetValue(idWay, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmWays[(long)change.Id!] = (Way)change;
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                        {
                            if (OsmRelations.TryGetValue(idRelation, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmRelations[(long)change.Id!] = (Relation)change;
                        }
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
                            if (OsmNodes.TryGetValue(idNode, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmNodes[idNode] = null;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (change.Id is long idWay)
                        {
                            if (OsmWays.TryGetValue(idWay, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmWays[idWay] = null;
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (change.Id is long idRelation)
                        {
                            if (OsmRelations.TryGetValue(idRelation, out var toBeDeleted) && (toBeDeleted?.Version ?? 0) > change.Version)
                                continue;
                            OsmRelations[idRelation] = null;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public void Build()
        {
            Nodes = OsmNodes.ToDictionary(x => x.Key, x => x.Value == null ? null : new PbfParsing.Node(x.Key, (double)x.Value.Latitude!, (double)x.Value.Longitude!, x.Value.Tags));
            Ways = OsmWays.ToDictionary(x => (uint)x.Key, x => x.Value == null ? null : new PbfParsing.Way(x.Key, x.Value.Nodes, x.Value.Tags));
            Relations = OsmRelations.ToDictionary(x => (uint)x.Key, x => x.Value == null ? null : new PbfParsing.Relation(x.Key, x.Value.Members.Select(m => new PbfParsing.RelationMember(m.Id, m.Role, (PbfParsing.OsmGeoType)m.Type)).ToArray(), x.Value.Tags));
        }
    }
}
