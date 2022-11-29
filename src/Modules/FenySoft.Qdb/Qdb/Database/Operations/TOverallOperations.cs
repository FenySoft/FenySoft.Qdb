using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class TOverallOperation : ITOperation
    {
        public TOverallOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public TOperationScope Scope
        {
            get { return TOperationScope.Overall; }
        }

        public ITData FromKey
        {
            get { return null; }
        }

        public ITData ToKey
        {
            get { return null; }
        }
    }

    public class TClearOperation : TOverallOperation
    {
        public TClearOperation()
            : base(TOperationCode.CLEAR)
        {
        }
    }
}
