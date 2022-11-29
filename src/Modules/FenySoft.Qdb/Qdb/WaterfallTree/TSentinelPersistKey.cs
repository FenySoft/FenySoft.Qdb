using FenySoft.Core.Persist;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public class TSentinelPersistKey : ITPersist<ITData>
    {
        public static readonly TSentinelPersistKey Instance = new TSentinelPersistKey();

        public void Write(BinaryWriter writer, ITData item)
        {
        }

        public ITData Read(BinaryReader reader)
        {
            return null;
        }
    }
}
