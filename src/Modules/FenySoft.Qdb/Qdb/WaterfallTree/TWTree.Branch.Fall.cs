using System.Diagnostics;

using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private partial class TBranch
        {
            public volatile Task FallTask;

            private void DoFall(object state)
            {
                var tuple = (Tuple<TBranch, TBranchCache, int, TToken, TParams>)state;
                var branch = tuple.Item1;
                var cache = tuple.Item2;
                var level = tuple.Item3;
                var token = tuple.Item4;
                var param = tuple.Item5;
                if (!branch.IsNodeLoaded)
                    token.Semaphore.Wait();
                var node = branch.Node;
                Debug.Assert(branch == node.Branch);
//#if DEBUG
//                Debug.Assert(node.TaskID == 0);
//                node.TaskID = Task.CurrentId.Value;
//#endif

                //if (param.TWalkAction != TWalkAction.CacheFlush)
                    node.Touch(level);

                //1. Apply cache
                if (cache != null)
                {
                    if (cache.Count == 1 || node.Type == NodeType.Leaf)
                    {
                        foreach (var kv in cache)
                        {
                            //compact operations
                            kv.Key.Apply.Internal(kv.Value);

                            //apply
                            if (kv.Value.Count > 0)
                                node.Apply(kv.Value);
                        }
                    }
                    else
                    {
                        Parallel.ForEach(cache, (kv) =>
                        {
                            //compact operations
                            kv.Key.Apply.Internal(kv.Value);

                            //apply
                            if (kv.Value.Count > 0)
                                node.Apply(kv.Value);
                        });
                    }

                    cache.Clear();
                }
                
                //2. Maintenance
                if (node.Type == NodeType.Internal)
                    ((TInternalNode)node).Maintenance(level, token);
                branch.NodeState = node.State;

                if (node.IsExpiredFromCache && (param.WalkAction & TWalkAction.CacheFlush) == TWalkAction.CacheFlush)
                    param = new TParams(param.WalkMethod, TWalkAction.Store | TWalkAction.Unload, param.WalkParams, param.Sink);

                if (node.Type == NodeType.Internal)
                {
                    if (param.WalkMethod != TWalkMethod.Current)
                    {
                        //broadcast
                        ((TInternalNode)node).BroadcastFall(level, token, param);
                    }
                }

                if ((param.WalkAction & TWalkAction.Store) == TWalkAction.Store)
                {
                    if (node.IsModified)
                        node.Store();
                }

                if ((param.WalkAction & TWalkAction.Unload) == TWalkAction.Unload)
                {
                    //node.IsExpiredFromCache = false;
                    branch.Node = null;
                    Tree.Exclude(branch.NodeHandle);
                }

                if (token != null)
                    token.CountdownEvent.Signal();
//#if DEBUG
//                node.TaskID = 0;
//#endif
                branch.FallTask = null;

                Tree.WorkingFallCount.Decrement();
            }

            public bool Fall(int level, TToken token, TParams param, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None)
            {
                lock (this)
                {
                    WaitFall();

                    if (token != null)
                    {
                        if (token.Cancellation.IsCancellationRequested)
                            return false;

                        token.CountdownEvent.AddCount(1);
                    }

                    bool haveSink = false;
                    TBranchCache cache = null;
                    if (param.Sink)
                    {
                        if (Cache.OperationCount > 0)
                        {
                            if (param.IsTotal)
                            {
                                cache = Cache;
                                Cache = new TBranchCache();
                                haveSink = true;
                            }
                            else //no matter IsOverall or IsPoint, we exclude all the operations for the path
                            {
                                ITOperationCollection operationCollection = Cache.Exclude(param.Path);
                                if (operationCollection != null)
                                {
                                    cache = new TBranchCache(/*param.Path,*/ operationCollection);
                                    haveSink = true;
                                }
                            }
                        }
                    }

                    Tree.WorkingFallCount.Increment();
                    FallTask = Task.Factory.StartNew(DoFall, new Tuple<TBranch, TBranchCache, int, TToken, TParams>(this, cache, level - 1, token, param), taskCreationOptions);

                    return haveSink;
                }
            }

            public void WaitFall()
            {
                lock (this)
                {
                    Task task = FallTask;
                    if (task != null)
                        task.Wait();
                }
            }

            public void ApplyToCache(TLocator locator, ITOperation operation)
            {
                lock (this)
                    Cache.Apply(locator, operation);
            }

            public void ApplyToCache(ITOperationCollection operations)
            {
                lock (this)
                    Cache.Apply(operations);
            }

            public void MaintenanceRoot(TToken token)
            {
                if (node.IsOverflow)
                {
                    TBranch newBranch = new TBranch(Tree, NodeType, NodeHandle);
                    newBranch.Node = Node;
                    newBranch.Node.Branch = newBranch;
                    newBranch.NodeState = newBranch.Node.State;

                    NodeType = NodeType.Internal;
                    //NodeHandle = Tree.Repository.Reserve();
                    NodeHandle = Tree.heap.ObtainNewHandle();
                    Node = TNode.Create(this);
                    NodeState = NodeState.None;

                    Tree.Depth++;

                    TInternalNode rootNode = (TInternalNode)Node;
                    rootNode.Branches.Add(new TFullKey(Tree.MinLocator, null), newBranch);
                    //rootNode.Branches.Add(newBranch.TNode.FirstKey, newBranch);
                    rootNode.HaveChildrenForMaintenance = true;
                    rootNode.Maintenance(Tree.Depth + 1, token);
                }
                else if (node.IsUnderflow)
                {
                    //TODO: also to release handle
                    //Debug.Assert(node.Type == NodeType.Internal);

                    //TBranch branch = ((TInternalNode)TNode).Branches[0].Value;

                    //NodeType = branch.NodeType;
                    //NodeHandle = branch.NodeHandle;
                    //TNode = branch.node;
                    //NodeState = branch.NodeState;

                    //Tree.Depth--;
                }

                token.CountdownEvent.Signal();
            }
        }

        private enum TWalkMethod
        {
            Current,
            CascadeFirst,
            CascadeLast,
            Cascade,
            CascadeButOnlyLoaded,
        }

        [Flags]
        private enum TWalkAction
        {
            None = 0,
            Store = 0x01,
            Unload = 0x02,
            CacheFlush = 0x04
        }

        private class TWalkParams
        {
        }

        private class TCacheWalkParams : TWalkParams
        {
            public long TouchID;

            public TCacheWalkParams(long touchID)
            {
                TouchID = touchID;
            }
        }

        private class TParams
        {
            public readonly TWalkMethod WalkMethod;
            public readonly TWalkAction WalkAction;
            public readonly TWalkParams WalkParams;

            #region param scope

            public readonly TLocator Path;
            public readonly ITData FromKey;
            public readonly ITData ToKey;
            public readonly bool IsPoint;
            public readonly bool IsOverall;
            public readonly bool IsTotal;

            #endregion

            public readonly bool Sink;

            public TParams(TWalkMethod walkMethod, TWalkAction walkAction, TWalkParams walkParams, bool sink, TLocator path, ITData fromKey, ITData toKey)
            {
                WalkMethod = walkMethod;
                WalkAction = walkAction;
                WalkParams = walkParams;

                Sink = sink;

                Path = path;
                FromKey = fromKey;
                ToKey = toKey;
                IsPoint = false;
                IsOverall = false;
                IsTotal = false;
            }

            public TParams(TWalkMethod walkMethod, TWalkAction walkAction, TWalkParams walkParams, bool sink, TLocator path, ITData key)
            {
                WalkMethod = walkMethod;
                WalkAction = walkAction;
                WalkParams = walkParams;

                Sink = sink;

                Path = path;
                FromKey = key;
                ToKey = key;
                IsPoint = true;
                IsOverall = false;
                IsTotal = false;
            }

            public TParams(TWalkMethod walkMethod, TWalkAction walkAction, TWalkParams walkParams, bool sink, TLocator path)
            {
                WalkMethod = walkMethod;
                WalkAction = walkAction;
                WalkParams = walkParams;

                Sink = sink;

                Path = path;
                IsPoint = false;
                IsOverall = true;
                IsTotal = false;
            }

            public TParams(TWalkMethod walkMethod, TWalkAction walkAction, TWalkParams walkParams, bool sink)
            {
                WalkMethod = walkMethod;
                WalkAction = walkAction;
                WalkParams = walkParams;

                Sink = sink;

                IsPoint = false;
                IsOverall = false;
                IsTotal = true;
            }
        }

        private class TToken
        {
            //private static long globalID = 0;

            //public readonly long ID;
            public readonly CountdownEvent CountdownEvent = new CountdownEvent(1);
            public readonly SemaphoreSlim Semaphore;
            public readonly CancellationToken Cancellation;

            [DebuggerStepThrough]
            public TToken(SemaphoreSlim semaphore, CancellationToken cancellationToken)
            {
                Semaphore = semaphore;
                Cancellation = cancellationToken;

                //ID = Interlocked.Increment(ref globalID);
            }
        }
    }
}