using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace OsmNightWatch
{
    public static class ProtobufHelpers
    {
        public static void ThrowOverflow()
        {
            throw new OverflowException();
        }
        public static void ThrowEoF()
        {
            throw new EndOfStreamException();
        }
    }
}
