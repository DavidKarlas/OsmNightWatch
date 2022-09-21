using OsmNightWatch.PbfParsing;
using OsmSharp.Replication;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    static class Utils
    {
        [ModuleInitializer]
        public static void HackOsmReplicationBug()
        {
            AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;

            Assembly? OnAssemblyResolve(AssemblyLoadContext arg1, AssemblyName arg2)
            {
                var assembly = Assembly.Load(new AssemblyName(arg2.Name));
                if (assembly != null)
                {
                    return assembly;
                }
                return null;
            }
        }

        public static Task<long> GetSequenceNumberFromPbf(PbfIndex pbfIndex)
        {
            var offset = pbfIndex.GetLastNodeOffset();
            var lastNodesWithMeta = NodesParser.LoadNodesWithMetadata(pbfIndex.PbfPath, offset).Last();
            if (lastNodesWithMeta.TimeStamp is not DateTime datetime)
                throw new NotSupportedException();
            return ReplicationConfig.Minutely.GuessSequenceNumberAt(datetime);
        }
    }
}
