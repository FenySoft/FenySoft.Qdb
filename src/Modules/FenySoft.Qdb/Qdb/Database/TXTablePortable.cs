using System.Collections;

using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class TXTablePortable : ITTable<ITData, ITData>
    {
        private ITOperationCollection operations;

        public readonly TWTree Tree;
        public readonly TLocator Locator;
        public volatile bool IsModified;

        public readonly object SyncRoot = new object();

        //public event Apply.ReadOperationDelegate PendingRead;

        internal TXTablePortable(TWTree tree, TLocator locator)
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

        private void Execute(ITOperation operation)
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
            Execute(new TReplaceOperation(key, record));
        }

        public void InsertOrIgnore(ITData key, ITData record)
        {
            Execute(new TInsertOrIgnoreOperation(key, record));
        }

        public void Delete(ITData key)
        {
            Execute(new TDeleteOperation(key));
        }

        public void Delete(ITData fromKey, ITData toKey)
        {
            Execute(new TDeleteRangeOperation(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new TClearOperation());
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

                TWTree.TFullKey nearFullKey;
                bool hasNearLocator;
                TWTree.TFullKey lastVisitedFullKey = default(TWTree.TFullKey);

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

                TWTree.TFullKey nearFullKey;
                bool hasNearFullKey;
                TWTree.TFullKey lastVisitedFullKey = default(TWTree.TFullKey);
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

                TWTree.TFullKey nearFullKey;
                bool hasNearFullKey;
                ITOrderedSet<ITData, ITData> records;

                TWTree.TFullKey lastVisitedFullKey = new TWTree.TFullKey(Locator, to);
                records = Tree.FindData(Locator, Locator, hasTo ? to : null, Direction.Backward, out nearFullKey, out hasNearFullKey, ref lastVisitedFullKey);

                if (records == null)
                    yield break;

                while (records != null)
                {
                    Task task = null;
                    ITOrderedSet<ITData, ITData> recs = null;

                    //if (records.Count > 0)
                    //    lastVisitedFullKey = new TWTree.TFullKey(TLocator, records.First.Key);

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
