using OsmSharp;
using OsmSharp.Db;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public class OsmDatabase : IOsmGeoSource
    {
        private Dictionary<long, Relation> relevantRelations;
        private Dictionary<long, Way> relevantWays;
        private Dictionary<long, Node> relevantNodes;

        public OsmDatabase(Dictionary<long, Relation> relevantRelations, Dictionary<long, Way> relevantWays, Dictionary<long, Node> relevantNodes)
        {
            this.relevantRelations = relevantRelations;
            this.relevantWays = relevantWays;
            this.relevantNodes = relevantNodes;
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    return relevantNodes[id];
                case OsmGeoType.Way:
                    return relevantWays[id];
                case OsmGeoType.Relation:
                    return relevantRelations[id];
                default:
                    throw new InvalidOperationException();
            }
        }
    }
}
