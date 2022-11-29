using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class TRangeOperation : ITOperation
    {
        private readonly ITData from;
        private readonly ITData to;

        protected TRangeOperation(int action, ITData from, ITData to)
        {
            Code = action;
            this.from = from;
            this.to = to;
        }

        protected TRangeOperation(int action)
        {
            Code = action;
        }

        public int Code { get; private set; }

        public TOperationScope Scope
        {
            get { return TOperationScope.Range; }
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

    public class TDeleteRangeOperation : TRangeOperation
    {
        public TDeleteRangeOperation(ITData from, ITData to)
            : base(TOperationCode.DELETE_RANGE, from, to)
        {
        }
    }
}
