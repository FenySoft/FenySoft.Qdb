using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class OverallOperation : IOperation
    {
        public OverallOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public OperationScope Scope
        {
            get { return OperationScope.Overall; }
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

    public class ClearOperation : OverallOperation
    {
        public ClearOperation()
            : base(OperationCode.CLEAR)
        {
        }
    }
}
