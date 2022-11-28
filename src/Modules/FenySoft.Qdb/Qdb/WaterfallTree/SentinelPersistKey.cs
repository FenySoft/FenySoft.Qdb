using FenySoft.Core.Persist;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public class SentinelPersistKey : ITPersist<ITData>
    {
        public static readonly SentinelPersistKey Instance = new SentinelPersistKey();

        public void Write(BinaryWriter writer, ITData item)
        {
        }

        public ITData Read(BinaryReader reader)
        {
            return null;
        }
    }
}
