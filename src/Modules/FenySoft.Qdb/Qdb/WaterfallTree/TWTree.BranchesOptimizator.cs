using System.Diagnostics;
using System.Collections.Concurrent;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private class TBranchesOptimizator
        {
            private const int MAP_CAPACITY = 131072;
            private ConcurrentDictionary<TLocator, TRange> Map = new ConcurrentDictionary<TLocator, TRange>();
            private TBranchCollection Branches;

            public TBranchesOptimizator()
            {
            }

            public void Rebuild(TBranchCollection branches)
            {
                Branches = branches;
                Map = BuildRanges();
            }

            private ConcurrentDictionary<TLocator, TRange> BuildRanges()
            {
                ConcurrentDictionary<TLocator, TRange> map = new ConcurrentDictionary<TLocator, TRange>();
                var locator = Branches[0].Key.Locator;
                TRange range = new TRange(0, true);
                map[locator] = range;

                for (int i = 1; i < Branches.Count; i++)
                {
                    var newLocator = Branches[i].Key.Locator;

                    if (newLocator.Equals(locator))
                    {
                        range.LastIndex = i;
                        continue;
                    }

                    locator = newLocator;
                    map[locator] = range = new TRange(i, true);
                }

                return map;
            }

            public TRange FindRange(TLocator locator)
            {
                TRange range;

                if (Map.TryGetValue(locator, out range))
                    return range;

                int idx = Branches.BinarySearch(new TFullKey(locator, null));
                Debug.Assert(idx < 0);
                idx = ~idx - 1;
                Debug.Assert(idx >= 0);

                Map[locator] = range = new TRange(idx, false);

                if (Map.Count > MAP_CAPACITY)
                    Map = BuildRanges(); //TODO: background rebuild

                return range;
            }

            public int FindIndex(TRange range, TLocator locator, ITData key)
            {
                if (!range.IsBaseLocator)
                    return range.LastIndex;

                int cmp = locator.KeyComparer.Compare(key, Branches[range.LastIndex].Key.Key);
                if (cmp >= 0)
                    return range.LastIndex;

                if (range.FirstIndex == range.LastIndex)
                    return range.LastIndex - 1;

                int idx = Branches.BinarySearch(new TFullKey(locator, key), range.FirstIndex, range.LastIndex - range.FirstIndex, LightComparer.Instance);
                if (idx < 0)
                    idx = ~idx - 1;

                return idx;
            }

            private class LightComparer : IComparer<KeyValuePair<TFullKey, TBranch>>
            {
                public readonly static LightComparer Instance = new LightComparer();

                public int Compare(KeyValuePair<TFullKey, TBranch> x, KeyValuePair<TFullKey, TBranch> y)
                {
                    //Debug.Assert(x.Key.Path.Equals(y.Key.Path));

                    return x.Key.Locator.KeyComparer.Compare(x.Key.Key, y.Key.Key);
                }
            }
        }

        [DebuggerDisplay("FirstIndex = {FirstIndex}, LastIndex = {LastIndex}, IsBaseLocator = {IsBaseLocator}")]
        private class TRange
        {
            public int FirstIndex;
            public int LastIndex;
            public bool IsBaseLocator;

            public TRange(int firstIndex, bool baseLocator)
            {
                FirstIndex = firstIndex;
                LastIndex = firstIndex;
                IsBaseLocator = baseLocator;
            }
        }
    }
}
