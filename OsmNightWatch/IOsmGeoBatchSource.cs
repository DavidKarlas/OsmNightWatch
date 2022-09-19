using OsmSharp.Db;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public interface IOsmGeoBatchSource : IOsmGeoSource
    {
        void BatchLoad(HashSet<long> nodeIds, HashSet<long> wayIds, HashSet<long> relationIds);
    }
}
