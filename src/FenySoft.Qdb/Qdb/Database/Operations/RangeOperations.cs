using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class RangeOperation : IOperation
    {
        private readonly ITData from;
        private readonly ITData to;

        protected RangeOperation(int action, ITData from, ITData to)
        {
            Code = action;
            this.from = from;
            this.to = to;
        }

        protected RangeOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public OperationScope Scope
        {
            get { return OperationScope.Range; }
        }

        public ITData FromKey
        {
            get { return from; }
        }

        public ITData ToKey
        {
            get { return to; }
        }
    }

    public class DeleteRangeOperation : RangeOperation
    {
        public DeleteRangeOperation(ITData from, ITData to)
            : base(OperationCode.DELETE_RANGE, from, to)
        {
        }
    }
}
