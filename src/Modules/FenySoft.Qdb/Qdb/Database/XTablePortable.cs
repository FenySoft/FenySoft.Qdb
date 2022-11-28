using System.Collections;

using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class XTablePortable : ITTable<ITData, ITData>
    {
        private IOperationCollection operations;

        public readonly WTree Tree;
        public readonly Locator Locator;
        public volatile bool IsModified;

        public readonly object SyncRoot = new object();

        //public event Apply.ReadOperationDelegate PendingRead;

        internal XTablePortable(WTree tree, Locator locator)
        {
            Tree = tree;
            Locator = locator;

            operations = locator.OperationCollectionFactory.Create(256);

            //((Apply)Path.DataDescriptor.Apply).ReadCallback += new Apply.ReadOperationDelegate(Apply_ReadCallback);
        }

        //~XIndex()
        //{
        //    Flush();
        //}

        //private void Apply_ReadCallback(long handle, bool exist, Path path, IKey key, IRecord record)
        //{
        //    if (!Path.Equals(path))
        //        return;

        //    if (PendingRead != null)
        //        PendingRead(handle, exist, path, key, record);
        //}

        //private void Read(IKey key, long handle)
        //{
        //    InternalExecute(new ReadOperation(key, handle));
        //}

        private void Execute(IOperation operation)
        {
            lock (SyncRoot)
            {
                IsModified = true;

                if (operations.Capacity == 0)
                {
                    Tree.Execute(Locator, operation);
                    return;
                }

                operations.Add(operation);
                if (operations.Count == operations.Capacity)
                    Flush();
            }
        }

        public void Flush()
        {
            lock (SyncRoot)
            {
                if (operations.Count == 0)
                    return;

                Tree.Execute(operations);

                operations.Clear();
            }
        }

        #region ITTable<IKey, IRecord>

        public ITData this[ITData key]
        {
            get
            {
                ITData record;
                if (!TryGet(key, out record))
                    throw new KeyNotFoundException(key.ToString());

                return record;
            }
            set
            {
                Replace(key, value);
            }
        }

        public void Replace(ITData key, ITData record)
        {
            Execute(new ReplaceOperation(key, record));
        }

        public void InsertOrIgnore(ITData key, ITData record)
        {
            Execute(new InsertOrIgnoreOperation(key, record));
        }

        public void Delete(ITData key)
        {
            Execute(new DeleteOperation(key));
        }

        public void Delete(ITData fromKey, ITData toKey)
        {
            Execute(new DeleteRangeOperation(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new ClearOperation());
        }

        public bool Exists(ITData key)
        {
            ITData record;
            return TryGet(key, out record);
        }

        public bool TryGet(ITData key, out ITData record)
        {
            lock (SyncRoot)
            {
                Flush();

                WTree.FullKey nearFullKey;
                bool hasNearLocator;
                WTree.FullKey lastVisitedFullKey = default(WTree.FullKey);

                var records = Tree.FindData(Locator, Locator, key, Direction.Forward, out nearFullKey, out hasNearLocator, ref lastVisitedFullKey);
                if (records == null)
                {
                    record = default(ITData);
                    return false;
                }

                lock (records)
                {
                    return records.TryGetValue(key, out record);
                }
            }
        }

        public ITData Find(ITData key)
        {
            ITData record;
            TryGet(key, out record);

            return record;
        }

        public ITData TryGetOrDefault(ITData key, ITData defaultRecord)
        {
            ITData record;
            if (!TryGet(key, out record))
                return defaultRecord;

            return record;
        }

        public KeyValuePair<ITData, ITData>? FindNext(ITData key)
        {
            lock (SyncRoot)
            {
                foreach (var kv in Forward(key, true, default(ITData), false))
                    return kv;

                return null;
            }
        }

        public KeyValuePair<ITData, ITData>? FindAfter(ITData key)
        {
            lock (SyncRoot)
            {
                var comparer = Locator.KeyComparer;

                foreach (var kv in Forward(key, true, default(ITData), false))
                {
                    if (comparer.Compare(kv.Key, key) == 0)
                        continue;

                    return kv;
                }

                return null;
            }
        }

        public KeyValuePair<ITData, ITData>? FindPrev(ITData key)
        {
            lock (SyncRoot)
            {
                foreach (var kv in Backward(key, true, default(ITData), false))
                    return kv;

                return null;
            }
        }

        public KeyValuePair<ITData, ITData>? FindBefore(ITData key)
        {
            lock (SyncRoot)
            {
                var comparer = Locator.KeyComparer;

                foreach (var kv in Backward(key, true, default(ITData), false))
                {
                    if (comparer.Compare(kv.Key, key) == 0)
                        continue;

                    return kv;
                }

                return null;
            }
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Forward()
        {
            return Forward(default(ITData), false, default(ITData), false);
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Forward(ITData from, bool hasFrom, ITData to, bool hasTo)
        {
            lock (SyncRoot)
            {
                var keyComparer = Locator.KeyComparer;

                if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                    throw new ArgumentException("from > to");

                Flush();

                WTree.FullKey nearFullKey;
                bool hasNearFullKey;
                WTree.FullKey lastVisitedFullKey = default(WTree.FullKey);
                ITOrderedSet<ITData, ITData> records;

                records = Tree.FindData(Locator, Locator, hasFrom ? from : null, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);

                if (records == null)
                {
                    if (!hasNearFullKey || !nearFullKey.Locator.Equals(Locator))
                        yield break;

                    records = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                }

                while (records != null) // && records.Count > 0
                {
                    Task task = null;
                    ITOrderedSet<ITData, ITData> recs = null;

                    if (hasNearFullKey && nearFullKey.Locator.Equals(Locator))
                    {
                        lock (records)
                        {
                            if (hasTo && records.Count > 0 && keyComparer.Compare(records.First.Key, to) > 0)
                                break;
                        }

                        task = Task.Factory.StartNew(() =>
                        {
                            recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Forward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                        });
                    }

                    lock (records)
                    {
                        foreach (var record in records.Forward(from, hasFrom, to, hasTo))
                            yield return record;
                    }

                    if (task != null)
                        task.Wait();

                    records = recs;
                }
            }
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Backward()
        {
            return Backward(default(ITData), false, default(ITData), false);
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Backward(ITData to, bool hasTo, ITData from, bool hasFrom)
        {
            lock (SyncRoot)
            {
                var keyComparer = Locator.KeyComparer;

                if (hasFrom && hasTo && keyComparer.Compare(from, to) > 0)
                    throw new ArgumentException("from > to");

                Flush();

                WTree.FullKey nearFullKey;
                bool hasNearFullKey;
                ITOrderedSet<ITData, ITData> records;

                WTree.FullKey lastVisitedFullKey = new WTree.FullKey(Locator, to);
                records = Tree.FindData(Locator, Locator, hasTo ? to : null, Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);

                if (records == null)
                    yield break;

                while (records != null)
                {
                    Task task = null;
                    ITOrderedSet<ITData, ITData> recs = null;

                    //if (records.Count > 0)
                    //    lastVisitedFullKey = new WTree.FullKey(Locator, records.First.Key);

                    if (hasNearFullKey)
                    {
                        lock (records)
                        {
                            if (hasFrom && records.Count > 0 && keyComparer.Compare(records.Last.Key, from) < 0)
                                break;
                        }

                        task = Task.Factory.StartNew(() =>
                        {
                            recs = Tree.FindData(Locator, nearFullKey.Locator, nearFullKey.Key, Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);
                        });
                    }

                    lock (records)
                    {
                        foreach (var record in records.Backward(to, hasTo, from, hasFrom))
                            yield return record;
                    }

                    if (task != null)
                        task.Wait();

                    if (recs == null)
                        break;

                    lock (records)
                    {
                        lock (recs)
                        {
                            if (recs.Count > 0 && records.Count > 0)
                            {
                                if (keyComparer.Compare(recs.First.Key, records.First.Key) >= 0)
                                    break;
                            }
                        }
                    }

                    records = recs;
                }
            }
        }

        public KeyValuePair<ITData, ITData> FirstRow
        {
            get { return Forward().First(); }
        }

        public KeyValuePair<ITData, ITData> LastRow
        {
            get { return Backward().First(); }
        }

        public long Count()
        {
            return this.LongCount();
        }

        public ITDescriptor Descriptor
        {
            get { return Locator; }
        }

        public IEnumerator<KeyValuePair<ITData, ITData>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public int OperationQueueCapacity
        {
            get
            {
                lock (SyncRoot)
                    return operations.Capacity;
            }
            set
            {
                lock (SyncRoot)
                {
                    Flush();

                    operations = Locator.OperationCollectionFactory.Create(value);
                }
            }
        }
    }
}
