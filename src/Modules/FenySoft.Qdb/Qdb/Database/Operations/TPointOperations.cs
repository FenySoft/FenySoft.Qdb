using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database.Operations
{
    public abstract class TPointOperation : ITOperation
    {
        private readonly ITData key;

        protected TPointOperation(int action, ITData key)
        {
            Code = action;
            this.key = key;
        }

        public int Code { get; private set; }

        public TOperationScope Scope
        {
            get { return TOperationScope.Point; }
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

    public class TDeleteOperation : TPointOperation
    {
        public TDeleteOperation(ITData key)
            : base(TOperationCode.DELETE, key)
        {
        }
    }

    public abstract class TValueOperation : TPointOperation
    {
        public ITData Record;

        public TValueOperation(int action, ITData key, ITData record)
            : base(action, key)
        {
            Record = record;
        }
    }

    public class TReplaceOperation : TValueOperation
    {
        public TReplaceOperation(ITData key, ITData record)
            : base(TOperationCode.REPLACE, key, record)
        {
        }
    }

    public class TInsertOrIgnoreOperation : TValueOperation
    {
        public TInsertOrIgnoreOperation(ITData key, ITData record)
            : base(TOperationCode.INSERT_OR_IGNORE, key, record)
        {
        }
    }
}
