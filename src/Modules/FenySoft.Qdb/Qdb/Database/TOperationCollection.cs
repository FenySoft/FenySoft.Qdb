using System.Diagnostics;

using FenySoft.Core.Data;
using FenySoft.Core.Extensions;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class TOperationCollection : List<ITOperation>, ITOperationCollection
    {
        public ITOperation[] Array
        {
            get { return this.GetArray(); }
        }
        
        public TOperationCollection(TLocator locator, ITOperation[] operations, int commonAction, bool areAllMonotoneAndPoint)
        {
            this.SetArray(operations);
            this.SetCount(operations.Length);

            Locator = locator;
            CommonAction = commonAction;
            AreAllMonotoneAndPoint = areAllMonotoneAndPoint;
        }

        public TOperationCollection(TLocator locator, int capacity)
            : base(capacity)
        {
            Locator = locator;
            CommonAction = TOperationCode.UNDEFINED;
            AreAllMonotoneAndPoint = true;
        }

        public new void Add(ITOperation operation)
        {
            if (AreAllMonotoneAndPoint)
            {
                if (Count == 0)
                {
                    AreAllMonotoneAndPoint = operation.Scope == TOperationScope.Point;
                    CommonAction = operation.Code;
                }
                else
                {
                    if (operation.Scope != TOperationScope.Point || Locator.KeyComparer.Compare(operation.FromKey, this[Count - 1].FromKey) <= 0)
                        AreAllMonotoneAndPoint = false;
                }
            }

            if (CommonAction != TOperationCode.UNDEFINED && CommonAction != operation.Code)
                CommonAction = TOperationCode.UNDEFINED;

            base.Add(operation);
        }

        public void AddRange(ITOperationCollection operations)
        {
            if (!operations.AreAllMonotoneAndPoint)
                AreAllMonotoneAndPoint = false;
            else
            {
                if (AreAllMonotoneAndPoint && Count > 0 && operations.Count > 0 && Locator.KeyComparer.Compare(this[Count - 1].FromKey, operations[0].FromKey) >= 0)
                    AreAllMonotoneAndPoint = false;
            }

            if (operations.CommonAction != CommonAction)
            {
                if (Count == 0)
                    CommonAction = operations.CommonAction;
                else if (operations.Count > 0)
                    CommonAction = TOperationCode.UNDEFINED;
            }

            var oprs = operations as TOperationCollection;

            if (oprs != null)
                this.AddRange(oprs.Array, 0, oprs.Count);
            else
            {
                foreach (var o in operations)
                    Add(o);
            }
        }

        public new void Clear()
        {
            base.Clear();
            CommonAction = TOperationCode.UNDEFINED;
            AreAllMonotoneAndPoint = true;
        }

        public ITOperationCollection Midlle(int index, int count)
        {
            ITOperation[] array = new ITOperation[count];
            System.Array.Copy(Array, index, array, 0, count);

            return new TOperationCollection(Locator, array, CommonAction, AreAllMonotoneAndPoint);
        }

        public int BinarySearch(ITData key, int index, int count)
        {
            Debug.Assert(AreAllMonotoneAndPoint);

            int low = index;
            int high = index + count - 1;

            var comparer = Locator.KeyComparer;

            while (low <= high)
            {
                int mid = (low + high) >> 1;
                int cmp = comparer.Compare(this[mid].FromKey, key);

                if (cmp == 0)
                    return mid;
                if (cmp < 0)
                    low = mid + 1;
                else
                    high = mid - 1;
            }

            return ~low;
        }

        public int CommonAction { get; private set; }
        public bool AreAllMonotoneAndPoint { get; private set; }

        public TLocator Locator { get; private set; }
    }
}
