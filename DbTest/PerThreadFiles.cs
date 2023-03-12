using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbTest
{
    static class PerThreadFiles
    {
        [ThreadStatic]
        static Dictionary<int, FileStream> _files = new Dictionary<int, FileStream>();

        public static FileStream GetFile(int id)
        {
            if (!_files.TryGetValue(id, out var file))
            {
                _files[id] = file = new FileStream(Path.Combine("NodesToWaysGroups", $"{id}", $"{Guid.NewGuid()}.txt"), FileMode.Create);
            }
            return file;
        }
    }
}
