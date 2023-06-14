using NetTopologySuite.Geometries;
using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

public class ProcessingAdmin
{
    public Relation Relation;
    public HashSet<long> Countries = new HashSet<long>();
    public List<Way> Ways = new List<Way>();
    public Geometry Polygon;

    public ProcessingAdmin(Relation relation)
    {
        this.Relation = relation;
        this.AdminLevel = int.Parse(relation.Tags!["admin_level"]);
    }

    public int AdminLevel { get;  }
}
