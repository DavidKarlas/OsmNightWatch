using OsmNightWatch.PbfParsing;

namespace OsmNightWatch.Analyzers.AdminCountPerCountry;

class ProcessingAdmin
{
    public Relation Admin;
    public HashSet<long> Countries = new HashSet<long>();
    public List<Way> Ways = new List<Way>();

    public ProcessingAdmin(Relation relation)
    {
        this.Admin = relation;
    }
}
