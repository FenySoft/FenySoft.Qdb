using FenySoft.Core.Data;

namespace FenySoft.Qdb.Database
{
    public class TXFile : TXStream
    {
        internal TXFile(ITTable<ITData, ITData> table)
            : base(table)
        {
        }
    }
}
