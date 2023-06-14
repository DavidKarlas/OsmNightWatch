using OsmSharp.Tags;
using System.Collections.Generic;

namespace OsmNightWatch.PbfParsing
{
    public class RelationMember
    {
        public RelationMember(long id, string role, OsmGeoType type)
        {
            Id = id;
            Role = role;
            Type = type;
        }

        public OsmGeoType Type { get; set; }

        public long Id { get; set; }

        public string Role { get; set; }

    }

    public enum OsmGeoType
    {
        Node,
        Way,
        Relation
    }

    public abstract class OsmGeo
    {
        public long Id { get; set; }
        public TagsCollectionBase? Tags { get; set; }
        public abstract OsmGeoType Type { get; }

        public OsmGeo(long id, TagsCollectionBase? tags)
        {
            Id = id;
            Tags = tags;
        }
    }

    public class Node : OsmGeo
    {
        public Node(long id, double lat, double lon, TagsCollectionBase? tags = null)
            : base(id, tags)
        {
            Latitude = lat;
            Longitude = lon;
        }

        public double Latitude { get; set; }
        public double Longitude { get; set; }

        public HashSet<uint>? ParentWays { get; set; }

        public override OsmGeoType Type => OsmGeoType.Node;
    }

    public class Way : OsmGeo
    {
        public Way(long id, long[] nodes, TagsCollectionBase? tags)
            : base(id, tags)
        {
            Nodes = nodes;
        }

        public long[] Nodes { get; set; }

        public HashSet<uint>? ParentRelations { get; set; }

        public override OsmGeoType Type => OsmGeoType.Way;
    }

    public class Relation : OsmGeo
    {
        public Relation(long id, RelationMember[] members, TagsCollectionBase? tags)
            : base(id, tags)
        {
            Members = members;
        }

        public RelationMember[] Members { get; set; }

        public override OsmGeoType Type => OsmGeoType.Relation;
    }

    public interface IOsmGeoSource
    {
        OsmGeo Get(OsmGeoType type, long id);
        Node GetNode(long id);
        Way GetWay(long id);
        Relation GetRelation(long id);
    }

    public interface IOsmGeoBatchSource : IOsmGeoSource
    {
        //TODO: Changes this weird API to GetNodes, GetWays and GetRelations
        public (IReadOnlyCollection<Node> nodes, IReadOnlyCollection<Way> ways, IReadOnlyCollection<Relation> relations) BatchLoad(HashSet<long>? nodeIds = null, HashSet<long>? wayIds = null, HashSet<long>? relationIds = null);
    }
}
