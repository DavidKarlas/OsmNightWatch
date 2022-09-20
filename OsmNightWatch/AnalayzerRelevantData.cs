using OsmSharp;
using OsmSharp.Changesets;
using OsmSharp.Complete;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public class AnalayzerRelevantData
    {
        private readonly string analyzerName;

        public AnalayzerRelevantData(string analyzerName)
        {
            this.analyzerName = analyzerName;
        }

        public void Update(IEnumerable<Node> nodes, IEnumerable<CompleteWay> ways, IEnumerable<CompleteRelation> relations)
        {
            
        }

        public void Process(Changeset changeset, Action<OsmGeo> action)
        {
            
        }
    }
}
