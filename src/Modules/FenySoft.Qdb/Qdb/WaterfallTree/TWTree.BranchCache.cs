using System.Collections;
using System.Diagnostics;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private class TBranchCache : IEnumerable<KeyValuePair<TLocator, ITOperationCollection>>
        {
            private Dictionary<TLocator, ITOperationCollection> cache;
            private ITOperationCollection Operations;

            /// <summary>
            /// Number of all operations in cache
            /// </summary>
            public int OperationCount { get; private set; }

            public int Count { get; private set; }

            public TBranchCache()
            {
            }

            public TBranchCache(ITOperationCollection operations)
            {
                Operations = operations;
                Count = 1;
                OperationCount = operations.Count;
            }

            private ITOperationCollection Obtain(TLocator locator)
            {
                if (Count == 0)
                {
                    Operations = locator.OperationCollectionFactory.Create(0);
                    Debug.Assert(cache == null);
                    Count++;
                }
                else
                {
                    if (!Operations.Locator.Equals(locator))
                    {
                        if (cache == null)
                        {
                            cache = new Dictionary<TLocator, ITOperationCollection>();
                            cache[Operations.Locator] = Operations;
                        }

                        if (!cache.TryGetValue(locator, out Operations))
                        {
                            cache[locator] = Operations = locator.OperationCollectionFactory.Create(0);
                            Count++;
                        }
                    }
                }

                return Operations;
            }

            public void Apply(TLocator locator, ITOperation operation)
            {
                var operations = Obtain(locator);

                operations.Add(operation);
                OperationCount++;
            }

            public void Apply(ITOperationCollection oprs)
            {
                var operations = Obtain(oprs.Locator);

                operations.AddRange(oprs);
                OperationCount += oprs.Count;
            }

            public void Clear()
            {
                cache = null;
                Operations = null;
                Count = 0;
                OperationCount = 0;
            }

            public bool Contains(TLocator locator)
            {
                if (Count == 0)
                    return false;

                if (Count == 1)
                    return Operations.Locator.Equals(locator);

                if (cache != null)
                    return cache.ContainsKey(locator);

                return false;
            }
                        
            public ITOperationCollection Exclude(TLocator locator)
            {
                if (Count == 0)
                    return null;

                ITOperationCollection operations;

                if (!Operations.Locator.Equals(locator))
                {
                    if (cache == null || !cache.TryGetValue(locator, out operations))
                        return null;

                    cache.Remove(locator);
                    if (cache.Count == 1)
                        cache = null;
                }
                else
                {
                    operations = Operations;

                    if (Count == 1)
                        Operations = null;
                    else
                    {
                        cache.Remove(locator);
                        Operations = cache.First().Value;
                        if (cache.Count == 1)
                            cache = null;
                    }
                }

                Count--;
                OperationCount -= operations.Count;

                return operations;
            }

            public IEnumerator<KeyValuePair<TLocator, ITOperationCollection>> GetEnumerator()
            {
                IEnumerable<KeyValuePair<TLocator, ITOperationCollection>> enumerable;

                if (Count == 0)
                    enumerable = Enumerable.Empty<KeyValuePair<TLocator, ITOperationCollection>>();
                else if (Count == 1)
                    enumerable = new KeyValuePair<TLocator, ITOperationCollection>[] { new KeyValuePair<TLocator, ITOperationCollection>(Operations.Locator, Operations) };
                else
                    enumerable = cache;

                return enumerable.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void Store(TWTree tree, BinaryWriter writer)
            {
                writer.Write(Count);
                if (Count == 0)
                    return;

                //write cache
                foreach (var kv in this)
                {
                    var locator = kv.Key;
                    var operations = kv.Value;

                    //write locator
                    tree.SerializeLocator(writer, locator);

                    //write operations
                    locator.OperationsPersist.Write(writer, operations);
                }
            }

            public void Load(TWTree tree, BinaryReader reader)
            {
                int count = reader.ReadInt32();
                if (count == 0)
                    return;

                for (int i = 0; i < count; i++)
                {
                    //read locator
                    var locator = tree.DeserializeLocator(reader);

                    //read operations
                    var operations = locator.OperationsPersist.Read(reader);

                    Add(locator, operations);
                }
            }

            private void Add(TLocator locator, ITOperationCollection operations)
            {
                if (Count > 0)
                {
                    if (cache == null)
                    {
                        cache = new Dictionary<TLocator, ITOperationCollection>();
                        cache[Operations.Locator] = Operations;
                    }

                    cache.Add(locator, operations);
                }

                Operations = operations;

                OperationCount += operations.Count;
                Count++;
            }
        }
    }
}
