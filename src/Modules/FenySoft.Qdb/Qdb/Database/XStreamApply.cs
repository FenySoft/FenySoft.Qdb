using System.Diagnostics;

using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class XStreamApply : IApply
    {
        const int BLOCK_SIZE = XStream.BLOCK_SIZE;

        public XStreamApply(Locator path)
        {
            Locator = path;
        }

        public bool Internal(IOperationCollection operations)
        {
            return false;
        }

        public bool Leaf(IOperationCollection operations, ITOrderedSet<ITData, ITData> data)
        {
            bool isModified = false;

            foreach (var opr in operations)
            {
                switch (opr.Code)
                {
                    case OperationCode.REPLACE:
                        {
                            if (Replace(data, (ReplaceOperation)opr))
                                isModified = true;
                        }
                        break;
                    case OperationCode.DELETE:
                        {
                            DeleteRangeOperation o = new DeleteRangeOperation(opr.FromKey, opr.FromKey);
                            if (Delete(data, (DeleteRangeOperation)o))
                                isModified = true;
                        }
                        break;
                    case OperationCode.DELETE_RANGE:
                        {
                            if (Delete(data, (DeleteRangeOperation)opr))
                                isModified = true;
                        }
                        break;

                    default:
                        throw new NotSupportedException();
                }
            }
            return isModified;
        }

        public Locator Locator { get; private set; }

        private bool Replace(ITOrderedSet<ITData, ITData> set, ReplaceOperation operation)
        {
            Debug.Assert(operation.Scope == OperationScope.Point);

            long from = ((TData<long>)operation.FromKey).Value;
            int localFrom = (int)(from % BLOCK_SIZE);
            long baseFrom = from - localFrom;
            TData<long> baseKey = new TData<long>(baseFrom);

            byte[] src = ((TData<byte[]>)operation.Record).Value;
            Debug.Assert(src.Length <= BLOCK_SIZE);
            Debug.Assert(baseFrom == BLOCK_SIZE * ((from + src.Length - 1) / BLOCK_SIZE));

            ITData tmp;
            if (set.TryGetValue(baseKey, out tmp))
            {
                TData<byte[]> rec = (TData<byte[]>)tmp;

                if (localFrom == 0 && src.Length >= rec.Value.Length)
                    rec.Value = src;
                else
                {
                    Debug.Assert(src.Length < BLOCK_SIZE);
                    byte[] dst = rec.Value;
                    if (dst.Length > localFrom + src.Length)
                        src.CopyTo(dst, localFrom);
                    else
                    {
                        byte[] buffer = new byte[localFrom + src.Length];
                        dst.CopyTo(buffer, 0);
                        src.CopyTo(buffer, localFrom);
                        rec.Value = buffer;
                    }
                }
            }
            else // if element with baseKey is not found
            {
                if (localFrom == 0)
                    set[baseKey] = new TData<byte[]>(src);
                else
                {
                    byte[] values = new byte[localFrom + src.Length];
                    src.CopyTo(values, localFrom);
                    set[baseKey] = new TData<byte[]>(values);
                }
            }

            return true;
        }

        private bool Delete(ITOrderedSet<ITData, ITData> set, DeleteRangeOperation operation)
        {
            long from = ((TData<long>)operation.FromKey).Value;
            long to = ((TData<long>)operation.ToKey).Value;

            int localFrom = (int)(from % BLOCK_SIZE);
            int localTo = (int)(to % BLOCK_SIZE);
            long baseFrom = from - localFrom;
            long baseTo = to - localTo;

            long internalFrom = localFrom > 0 ? baseFrom + BLOCK_SIZE : baseFrom;
            long internalTo = localTo < BLOCK_SIZE - 1 ? baseTo - 1 : baseTo;

            bool isModified = false;

            if (internalFrom <= internalTo)
                isModified = set.Remove(new TData<long>(internalFrom), true, new TData<long>(internalTo), true);

            ITData tmp;
            TData<byte[]> record;

            if (localFrom > 0 && set.TryGetValue(new TData<long>(baseFrom), out tmp))
            {
                record = (TData<byte[]>)tmp;
                if (localFrom < record.Value.Length)
                {
                    Array.Clear(record.Value, localFrom, baseFrom < baseTo ? record.Value.Length - localFrom : localTo - localFrom + 1);
                    isModified = true;
                }
                if (baseFrom == baseTo)
                    return isModified;
            }

            if (localTo < BLOCK_SIZE - 1 && set.TryGetValue(new TData<long>(baseTo), out tmp))
            {
                record = (TData<byte[]>)tmp;
                if (localTo < record.Value.Length - 1)
                {
                    Array.Clear(record.Value, 0, localTo + 1);
                    isModified = true;
                }
                else
                    isModified = set.Remove(new TData<long>(baseTo));
            }

            return isModified;
        }
    }
}
