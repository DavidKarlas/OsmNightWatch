using OsmSharp;

namespace OsmNightWatch.Analyzers.AdminCountPerAdmin2;

class ProcessingAdmin
{
    public Relation Admin;
    public HashSet<long> Countries = new HashSet<long>();

    public ProcessingAdmin(Relation relation)
    {
        this.Admin = relation;
    }
}
