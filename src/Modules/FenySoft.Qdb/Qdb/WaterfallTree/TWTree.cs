using System.Collections.Concurrent;
using System.Diagnostics;
using FenySoft.Core.Threading;
using FenySoft.Core.Data;
using FenySoft.Core.Collections;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree : IDisposable
    {
        public int INTERNAL_NODE_MIN_BRANCHES = 2; //default values
        public int INTERNAL_NODE_MAX_BRANCHES = 5;
        public int INTERNAL_NODE_MAX_OPERATIONS_IN_ROOT = 8 * 1024;
        public int INTERNAL_NODE_MIN_OPERATIONS = 32 * 1024;
        public int INTERNAL_NODE_MAX_OPERATIONS = 64 * 1024;
        public int LEAF_NODE_MIN_RECORDS = 8 * 1024;
        public int LEAF_NODE_MAX_RECORDS = 64 * 1024;

        //reserved handles
        private const long HANDLE_SETTINGS = 0;
        private const long HANDLE_SCHEME = 1;
        private const long HANDLE_ROOT = 2;
        private const long HANDLE_RESERVED = 3;

        private readonly TCountdown WorkingFallCount = new TCountdown();
        private readonly TBranch RootBranch;
        private bool isRootCacheLoaded;

        private volatile bool disposed = false;
        private volatile bool Shutdown = false;
        private int Depth = 1;

        private long globalVersion;

        public long GlobalVersion
        {
            get { return Interlocked.Read(ref globalVersion); }
            set { Interlocked.Exchange(ref globalVersion, value); }
        }

        private readonly TScheme scheme;
        public readonly ITHeap heap;

        public TWTree(ITHeap heap)
        {
            if (heap == null)
                throw new NullReferenceException("heap");

            this.heap = heap;

            if (heap.Exists(HANDLE_SETTINGS))
            {
                //create root branch with dummy handle
                RootBranch = new TBranch(this, NodeType.Leaf, 0);

                //read settings - settings will set the RootBranch.NodeHandle
                using (MemoryStream ms = new MemoryStream(heap.Read(HANDLE_SETTINGS)))
                    TSettings.Deserialize(this, ms);

                //read scheme
                using (MemoryStream ms = new MemoryStream(heap.Read(HANDLE_SCHEME)))
                    scheme = TScheme.Deserialize(new BinaryReader(ms));

                ////load branch cache
                //using (MemoryStream ms = new MemoryStream(THeap.Read(HANDLE_ROOT)))
                //    RootBranch.TCache.Load(this, new BinaryReader(ms));
                isRootCacheLoaded = false;
            }
            else
            {
                //obtain reserved handles
                var handle = heap.ObtainNewHandle();
                if (handle != HANDLE_SETTINGS)
                    throw new Exception("Logical error.");

                scheme = new TScheme();
                handle = heap.ObtainNewHandle();
                if (handle != HANDLE_SCHEME)
                    throw new Exception("Logical error.");

                handle = heap.ObtainNewHandle();
                if (handle != HANDLE_ROOT)
                    throw new Exception("Logical error.");

                handle = heap.ObtainNewHandle();
                if (handle != HANDLE_RESERVED)
                    throw new Exception("Logical error.");

                RootBranch = new TBranch(this, NodeType.Leaf); //the constructor will invoke THeap.ObtainHandle()

                isRootCacheLoaded = true;
            }

            CacheThread = new Thread(DoCache);
            CacheThread.Start();
        }

        private void LoadRootCache()
        {
            using (MemoryStream ms = new MemoryStream(heap.Read(HANDLE_ROOT)))
                RootBranch.Cache.Load(this, new BinaryReader(ms));

            isRootCacheLoaded = true;
        }

        private void Sink()
        {
            RootBranch.WaitFall();

            if (RootBranch.NodeState != NodeState.None)
            {
                TToken token = new TToken(CacheSemaphore, new CancellationTokenSource().Token);
                RootBranch.MaintenanceRoot(token);
                RootBranch.Node.Touch(Depth + 1);
                token.CountdownEvent.Wait();
            }

            RootBranch.Fall(Depth + 1, new TToken(CacheSemaphore, CancellationToken.None), new TParams(TWalkMethod.Current, TWalkAction.None, null, true));
        }

        public void Execute(ITOperationCollection operations)
        {
            if (disposed)
                throw new ObjectDisposedException("TWTree");

            lock (RootBranch)
            {
                if (!isRootCacheLoaded)
                    LoadRootCache();
                
                RootBranch.ApplyToCache(operations);

                if (RootBranch.Cache.OperationCount > INTERNAL_NODE_MAX_OPERATIONS_IN_ROOT)
                    Sink();
            }
        }

        public void Execute(TLocator locator, ITOperation operation)
        {
            if (disposed)
                throw new ObjectDisposedException("TWTree");

            lock (RootBranch)
            {
                if (!isRootCacheLoaded)
                    LoadRootCache();
                
                RootBranch.ApplyToCache(locator, operation);

                if (RootBranch.Cache.OperationCount > INTERNAL_NODE_MAX_OPERATIONS_IN_ROOT)
                    Sink();
            }
        }

        /// <summary>
        /// The hook.
        /// </summary>
        public ITOrderedSet<ITData, ITData> FindData(TLocator originalLocator, TLocator locator, ITData key, Direction direction, out TFullKey nearFullKey, out bool hasNearFullKey, ref TFullKey lastVisitedFullKey)
        {
            if (disposed)
                throw new ObjectDisposedException("TWTree");

            nearFullKey = default(TFullKey);
            hasNearFullKey = false;

            var branch = RootBranch;
            Monitor.Enter(branch);

            if (!isRootCacheLoaded)
                LoadRootCache();

            TParams param;
            if (key != null)
                param = new TParams(TWalkMethod.Cascade, TWalkAction.None, null, true, locator, key);
            else
            {
                switch (direction)
                {
                    case Direction.Forward:
                        param = new TParams(TWalkMethod.CascadeFirst, TWalkAction.None, null, true, locator);
                        break;
                    case Direction.Backward:
                        param = new TParams(TWalkMethod.CascadeLast, TWalkAction.None, null, true, locator);
                        break;
                    default:
                        throw new NotSupportedException(direction.ToString());
                }
            }

            branch.Fall(Depth + 1, new TToken(CacheSemaphore, CancellationToken.None), param);
            branch.WaitFall();

            switch (direction)
            {
                case Direction.Forward:
                    {
                        while (branch.NodeType == NodeType.Internal)
                        {
                            KeyValuePair<TFullKey, TBranch> newBranch = ((TInternalNode)branch.Node).FindBranch(locator, key, direction, ref nearFullKey, ref hasNearFullKey);

                            Monitor.Enter(newBranch.Value);
                            newBranch.Value.WaitFall();
                            Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                            Monitor.Exit(branch);

                            branch = newBranch.Value;
                        }
                    }
                    break;
                case Direction.Backward:
                    {
                        int depth = Depth;
                        KeyValuePair<TFullKey, TBranch> newBranch = default(KeyValuePair<TFullKey, TBranch>);
                        while (branch.NodeType == NodeType.Internal)
                        {
                            TInternalNode node = (TInternalNode)branch.Node;
                            newBranch = node.Branches[node.Branches.Count - 1];

                            int cmp = newBranch.Key.Locator.CompareTo(lastVisitedFullKey.Locator);
                            if (cmp == 0)
                            {
                                if (lastVisitedFullKey.Key == null)
                                    cmp = -1;
                                else
                                    cmp = newBranch.Key.Locator.KeyComparer.Compare(newBranch.Key.Key, lastVisitedFullKey.Key);
                            }
                            //else
                            //{
                            //    Debug.WriteLine("");
                            //}

                            //newBranch.Key.CompareTo(lastVisitedFullKey) >= 0
                            if (cmp >= 0)
                                newBranch = node.FindBranch(locator, key, direction, ref nearFullKey, ref hasNearFullKey);
                            else
                            {
                                if (node.Branches.Count >= 2)
                                {
                                    hasNearFullKey = true;
                                    nearFullKey = node.Branches[node.Branches.Count - 2].Key;
                                }
                            }
                            
                            Monitor.Enter(newBranch.Value);
                            depth--;
                            newBranch.Value.WaitFall();
                            if (newBranch.Value.Cache.Contains(originalLocator))
                            {
                                newBranch.Value.Fall(depth + 1, new TToken(CacheSemaphore, CancellationToken.None), new TParams(TWalkMethod.Current, TWalkAction.None, null, true, originalLocator));
                                newBranch.Value.WaitFall();
                            }
                            Debug.Assert(!newBranch.Value.Cache.Contains(originalLocator));
                            Monitor.Exit(branch);

                            branch = newBranch.Value;
                        }

                        //if (lastVisitedFullKey.TLocator.Equals(newBranch.Key.TLocator) &&
                        //    (lastVisitedFullKey.Key != null && lastVisitedFullKey.TLocator.KeyEqualityComparer.Equals(lastVisitedFullKey.Key, newBranch.Key.Key)))
                        //{
                        //    Monitor.Exit(branch);
                        //    return null;
                        //}

                        lastVisitedFullKey = newBranch.Key;
                    }
                    break;
                default:
                    throw new NotSupportedException(direction.ToString());
            }

            ITOrderedSet<ITData, ITData> data = ((TLeafNode)branch.Node).FindData(originalLocator, direction, ref nearFullKey, ref hasNearFullKey);

            Monitor.Exit(branch);

            return data;
        }

        private void Commit(CancellationToken cancellationToken, TLocator locator = default(TLocator), bool hasLocator = false, ITData fromKey = null, ITData toKey = null)
        {
            if (disposed)
                throw new ObjectDisposedException("TWTree");

            TParams param;
            if (!hasLocator)
                param = new TParams(TWalkMethod.CascadeButOnlyLoaded, TWalkAction.Store, null, false);
            else
            {
                if (fromKey == null)
                    param = new TParams(TWalkMethod.CascadeButOnlyLoaded, TWalkAction.Store, null, false, locator);
                else
                {
                    if (toKey == null)
                        param = new TParams(TWalkMethod.CascadeButOnlyLoaded, TWalkAction.Store, null, false, locator, fromKey);
                    else
                        param = new TParams(TWalkMethod.CascadeButOnlyLoaded, TWalkAction.Store, null, false, locator, fromKey, toKey);
                }
            }

            lock (RootBranch)
            {
                if (!isRootCacheLoaded)
                    LoadRootCache();
                
                TToken token = new TToken(CacheSemaphore, cancellationToken);
                RootBranch.Fall(Depth + 1, token, param);

                token.CountdownEvent.Signal();
                token.CountdownEvent.Wait();

                //write settings
                using (MemoryStream ms = new MemoryStream())
                {
                    TSettings.Serialize(this, ms);
                    heap.Write(HANDLE_SETTINGS, ms.GetBuffer(), 0, (int)ms.Length);
                }

                //write scheme
                using (MemoryStream ms = new MemoryStream())
                {
                    scheme.Serialize(new BinaryWriter(ms));
                    heap.Write(HANDLE_SCHEME, ms.GetBuffer(), 0, (int)ms.Length);
                }

                //write root cache
                using (MemoryStream ms = new MemoryStream())
                {
                    RootBranch.Cache.Store(this, new BinaryWriter(ms));
                    heap.Write(HANDLE_ROOT, ms.GetBuffer(), 0, (int)ms.Length);
                }

                heap.Commit();
            }
        }

        public virtual void Commit()
        {
            Commit(CancellationToken.None);
        }

        public ITHeap Heap
        {
            get { return heap; }
        }

        #region TLocator

        private TLocator MinLocator
        {
            get { return TLocator.MIN; }
        }

        protected TLocator CreateLocator(string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType)
        {
            return scheme.Create(name, structureType, keyDataType, recordDataType, keyType, recordType);
        }

        protected TLocator GetLocator(long id)
        {
            return scheme[id];
        }

        protected IEnumerable<TLocator> GetAllLocators()
        {
            return scheme.Select(kv => kv.Value);
        }

        private void SerializeLocator(BinaryWriter writer, TLocator locator)
        {
            writer.Write(locator.ID);
        }

        private TLocator DeserializeLocator(BinaryReader reader)
        {
            long id = reader.ReadInt64();
            if (id == TLocator.MIN.ID)
                return TLocator.MIN;

            TLocator locator = scheme[id];

            if (!locator.IsReady)
                locator.Prepare();

            if (locator == null)
                throw new Exception("Logical error");

            return locator;
        }

        #endregion

        #region TCache

        /// <summary>
        /// TBranch.NodeID -> node
        /// </summary>
        private readonly ConcurrentDictionary<long, TNode> Cache = new ConcurrentDictionary<long, TNode>();
        private Thread CacheThread;

        private SemaphoreSlim CacheSemaphore = new SemaphoreSlim(int.MaxValue, int.MaxValue);

        private int cacheSize = 32;

        public int CacheSize
        {
            get { return cacheSize; }
            set
            {
                if (value <= 0)
                    throw new ArgumentException("TCache size is invalid.");

                cacheSize = value;

                if (Cache.Count > CacheSize * 1.1)
                {
                    lock (Cache)
                        Monitor.Pulse(Cache);
                }
            }
        }

        private void Packet(long id, TNode node)
        {
            Debug.Assert(!Cache.ContainsKey(id));
            Cache[id] = node;

            if (Cache.Count > CacheSize * 1.1)
            {
                lock (Cache)
                    Monitor.Pulse(Cache);
            }
        }

        private TNode Retrieve(long id)
        {
            TNode node;
            Cache.TryGetValue(id, out node);

            return node;
        }

        private TNode Exclude(long id)
        {
            TNode node;
            Cache.TryRemove(id, out node);
            //Debug.Assert(node != null);

            int delta = (int)(CacheSize * 1.1 - Cache.Count);
            if (delta > 0)
                CacheSemaphore.Release(delta);

            return node;
        }

        private void DoCache()
        {
            while (!Shutdown)
            {
                while (Cache.Count > CacheSize * 1.1)
                {
                    KeyValuePair<long, TNode>[] kvs = Cache.ToArray();

                    foreach (var kv in kvs.Where(x => !x.Value.IsRoot).OrderBy(x => x.Value.TouchID).Take(Cache.Count - CacheSize))
                        kv.Value.IsExpiredFromCache = true;
                    //Debug.WriteLine(TCache.Count);
                    TToken token;
                    lock (RootBranch)
                    {
                        token = new TToken(CacheSemaphore, CancellationToken.None);
                        CacheSemaphore = new SemaphoreSlim(0, int.MaxValue);
                        var param = new TParams(TWalkMethod.CascadeButOnlyLoaded, TWalkAction.CacheFlush, null, false);
                        RootBranch.Fall(Depth + 1, token, param);
                    }

                    token.CountdownEvent.Signal();
                    token.CountdownEvent.Wait();
                    CacheSemaphore.Release(int.MaxValue / 2);
                }

                lock (Cache)
                {
                    if (Cache.Count <= CacheSize * 1.1)
                        Monitor.Wait(Cache, 1);
                }
            }
        }

        #endregion

        #region IDisposable Members

        private void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    Shutdown = true;
                    if (CacheThread != null)
                        CacheThread.Join();

                    WorkingFallCount.Wait();

                    heap.Close();
                }

                disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            
            GC.SuppressFinalize(this);
        }

        ~TWTree()
        {
            Dispose(false);
        }

        public virtual void Close()
        {
            Dispose();
        }

        #endregion

        public int GetMinimumlWTreeDepth(long recordCount)
        {
            int b = INTERNAL_NODE_MAX_BRANCHES;
            int R = INTERNAL_NODE_MAX_OPERATIONS_IN_ROOT;
            int I = INTERNAL_NODE_MAX_OPERATIONS;
            int L = LEAF_NODE_MAX_RECORDS;

            double depth = Math.Log(((recordCount - R) * (b - 1) + b * I) / (L * (b - 1) + I), b) + 1;

            return (int)Math.Ceiling(depth);
        }

        public int GetMaximumWTreeDepth(long recordCount)
        {
            int b = INTERNAL_NODE_MAX_BRANCHES;
            int L = LEAF_NODE_MAX_RECORDS;

            double depth = Math.Log(recordCount / L, b) + 1;

            return (int)Math.Ceiling(depth);
        }
    }

    public enum Direction
    {
        Backward = -1,
        None = 0,
        Forward = 1
    }
}
