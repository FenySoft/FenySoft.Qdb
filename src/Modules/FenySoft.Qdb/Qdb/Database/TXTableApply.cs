using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public sealed class TXTableApply : ITApply
    {
        public event ReadOperationDelegate ReadCallback;

        public delegate void ReadOperationDelegate(long handle, bool exist, TLocator path, ITData key, ITData record);

        public TXTableApply(TLocator locator)
        {
            Locator = locator;
        }

        public bool Internal(ITOperationCollection operations)
        {
            return false;
        }

        private bool SequentialApply(ITOperationCollection operations, ITOrderedSet<ITData, ITData> data)
        {
            switch (operations.CommonAction)
            {
                case TOperationCode.REPLACE:
                case TOperationCode.INSERT_OR_IGNORE:
                    {
                        foreach (var operation in operations)
                        {
                            TValueOperation opr = (TValueOperation)operation;
                            data.UnsafeAdd(opr.FromKey, opr.Record);
                        }

                        return true;
                    }
                case TOperationCode.DELETE:
                    {
                        return false;
                    }

                case TOperationCode.DELETE_RANGE:
                case TOperationCode.CLEAR:
                    {
                        throw new Exception("Logical error.");
                    }
                default:
                    throw new NotSupportedException();
            }
        }

        private bool CommonApply(ITOperationCollection operations, ITOrderedSet<ITData, ITData> data)
        {
            int commonAction = operations.CommonAction;

            int changes = 0;

            switch (commonAction)
            {
                case TOperationCode.REPLACE:
                    {
                        foreach (var opr in operations)
                        {
                            data[opr.FromKey] = ((TReplaceOperation)opr).Record;
                            changes++;
                        }
                    }
                    break;

                case TOperationCode.INSERT_OR_IGNORE:
                    {
                        foreach (var opr in operations)
                        {
                            if (data.ContainsKey(opr.FromKey))
                                continue;

                            data[opr.FromKey] = ((TInsertOrIgnoreOperation)opr).Record;
                            changes++;
                        }
                    }
                    break;

                case TOperationCode.DELETE:
                    {
                        foreach (var opr in operations)
                        {
                            if (data.Remove(opr.FromKey))
                                changes++;
                        }
                    }
                    break;

                case TOperationCode.DELETE_RANGE:
                    {
                        foreach (var opr in operations)
                        {
                            if (data.Remove(opr.FromKey, true, opr.ToKey, true))
                                changes++;
                        }
                    }
                    break;

                case TOperationCode.CLEAR:
                    {
                        foreach (var opr in operations)
                        {
                            data.Clear();
                            changes++;
                            break;
                        }
                    }
                    break;

                default:
                    throw new NotImplementedException();
            }

            return changes > 0;
        }

        public bool Leaf(ITOperationCollection operations, ITOrderedSet<ITData, ITData> data)
        {
            //sequential optimization
            if (operations.AreAllMonotoneAndPoint && data.IsInternallyOrdered && (data.Count == 0 || operations.Locator.KeyComparer.Compare(data.Last.Key, operations[0].FromKey) < 0))
                return SequentialApply(operations, data);

            //common action optimization
            if (operations.CommonAction != TOperationCode.UNDEFINED)
                return CommonApply(operations, data);

            //standart apply
            bool isModified = false;

            foreach (var opr in operations)
            {
                switch (opr.Code)
                {
                    case TOperationCode.REPLACE:
                        {
                            data[opr.FromKey] = ((TReplaceOperation)opr).Record;

                            isModified = true;
                        }
                        break;
                    case TOperationCode.INSERT_OR_IGNORE:
                        {
                            if (data.ContainsKey(opr.FromKey))
                                continue;

                            data[opr.FromKey] = ((TInsertOrIgnoreOperation)opr).Record;

                            isModified = true;
                        }
                        break;
                    case TOperationCode.DELETE:
                        {
                            if (data.Remove(opr.FromKey))
                                isModified = true;
                        }
                        break;
                    case TOperationCode.DELETE_RANGE:
                        {
                            if (data.Remove(opr.FromKey, true, opr.ToKey, true))
                                isModified = true;
                        }
                        break;
                    case TOperationCode.CLEAR:
                        {
                            data.Clear();
                            isModified = true;
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }

            return isModified;
        }

        public TLocator Locator { get; private set; }
    }
}
