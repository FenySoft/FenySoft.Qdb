using FenySoft.Core.Data;

namespace FenySoft.Qdb.Database
{
    public class XFile : XStream
    {
        internal XFile(ITTable<ITData, ITData> table)
            : base(table)
        {
        }
    }
}
