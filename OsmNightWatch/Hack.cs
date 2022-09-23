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
    static class Hack
    {
        [ModuleInitializer]
        public static void HackOsmReplicationBug()
        {
            AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;

            Assembly? OnAssemblyResolve(AssemblyLoadContext arg1, AssemblyName arg2)
            {
                if (arg2.Name == "OsmSharp.XmlSerializers")
                    return null;
                var assembly = Assembly.Load(new AssemblyName(arg2.Name));
                if (assembly != null)
                {
                    return assembly;
                }
                return null;
            }
        }
    }
}
