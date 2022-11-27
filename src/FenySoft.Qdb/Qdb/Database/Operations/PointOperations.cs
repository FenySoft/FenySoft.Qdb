using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class PointOperation : IOperation
    {
        private readonly ITData key;

        protected PointOperation(int action, ITData key)
        {
            Code = action;
            this.key = key;
        }

        public int Code { get; private set; }

        public OperationScope Scope
        {
            get { return OperationScope.Point; }
        }

        public ITData FromKey
        {
            get { return key; }
        }

        public ITData ToKey
        {
            get { return key; }
        }

        public override string ToString()
        {
            return ToKey.ToString();
        }
    }

    public class DeleteOperation : PointOperation
    {
        public DeleteOperation(ITData key)
            : base(OperationCode.DELETE, key)
        {
        }
    }

    public abstract class ValueOperation : PointOperation
    {
        public ITData Record;

        public ValueOperation(int action, ITData key, ITData record)
            : base(action, key)
        {
            Record = record;
        }
    }

    public class ReplaceOperation : ValueOperation
    {
        public ReplaceOperation(ITData key, ITData record)
            : base(OperationCode.REPLACE, key, record)
        {
        }
    }

    public class InsertOrIgnoreOperation : ValueOperation
    {
        public InsertOrIgnoreOperation(ITData key, ITData record)
            : base(OperationCode.INSERT_OR_IGNORE, key, record)
        {
        }
    }
}
