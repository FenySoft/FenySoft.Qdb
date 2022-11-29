using System.Diagnostics;

using FenySoft.Core.Compression;
using FenySoft.Core.Extensions;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private sealed partial class TInternalNode : TNode
        {
            public const byte VERSION = 40;

            private readonly TBranchesOptimizator Optimizator = new TBranchesOptimizator();

            public readonly TBranchCollection Branches;

            public TInternalNode(TBranch branch)
                : this(branch, new TBranchCollection(), false)
            {
            }

            public TInternalNode(TBranch branch, TBranchCollection branches, bool isModified)
                : base(branch)
            {
                Debug.Assert(branch.NodeType == NodeType.Internal);

                Branches = branches;
                IsModified = isModified;
            }

            private void SequentialApply(ITOperationCollection operations)
            {
                TLocator locator = operations.Locator;

                var last = Branches[Branches.Count - 1];
                if (ReferenceEquals(last.Key.Locator, locator) && locator.KeyComparer.Compare(last.Key.Key, operations[0].FromKey) <= 0)
                {
                    TBranch branch = last.Value;
                    branch.ApplyToCache(operations);
                    if (branch.NodeState != NodeState.None)
                        HaveChildrenForMaintenance = true;

                    return;
                }

                TRange range = Optimizator.FindRange(locator);

                if (!range.IsBaseLocator)
                {
                    TBranch branch = Branches[range.LastIndex].Value;
                    branch.ApplyToCache(operations);
                    if (branch.NodeState != NodeState.None)
                        HaveChildrenForMaintenance = true;

                    return;
                }

                int index = 0;

                for (int i = range.FirstIndex; i <= range.LastIndex; i++)
                {
                    var key = Branches[i].Key.Key;

                    int idx = operations.BinarySearch(key, index, operations.Count - index);
                    if (idx < 0)
                        idx = ~idx;
                    idx--;

                    int count = idx - index + 1;
                    if (count > 0)
                    {
                        var oprs = count < operations.Count ? operations.Midlle(index, count) : operations;
                        var branch = Branches[i - 1].Value;

                        branch.ApplyToCache(oprs);
                        if (branch.NodeState != NodeState.None)
                            HaveChildrenForMaintenance = true;

                        index += count;
                    }
                }

                if (operations.Count - index > 0)
                {
                    var oprs = index > 0 ? operations.Midlle(index, operations.Count - index) : operations;
                    var branch = Branches[range.LastIndex].Value;

                    Debug.Assert(Branches[range.LastIndex].Key.Locator.Equals(oprs.Locator));
                    Debug.Assert(oprs.Locator.KeyComparer.Compare(Branches[range.LastIndex].Key.Key, oprs[0].FromKey) <= 0);

                    branch.ApplyToCache(oprs);
                    if (branch.NodeState != NodeState.None)
                        HaveChildrenForMaintenance = true;
                }
            }

            public override void Apply(ITOperationCollection operations)
            {
                Debug.Assert(operations.Count > 0);

                if (operations.AreAllMonotoneAndPoint)
                {
                    SequentialApply(operations); //sequential mode optimization
                    IsModified = true;
                    return;
                }

                TLocator locator = operations.Locator;
                TRange range = Optimizator.FindRange(locator);

                foreach (var operation in operations)
                {
                    int firstIndex, lastIndex;

                    switch (operation.Scope)
                    {
                        case TOperationScope.Point:
                            {
                                firstIndex = lastIndex = Optimizator.FindIndex(range, locator, operation.FromKey);
                                Debug.Assert(firstIndex >= 0);
                            }
                            break;
                        case TOperationScope.Range:
                            {
                                firstIndex = Optimizator.FindIndex(range, locator, operation.FromKey);
                                if (firstIndex < 0)
                                    firstIndex = 0;
                                lastIndex = Optimizator.FindIndex(range, locator, operation.ToKey);
                            }
                            break;
                        case TOperationScope.Overall:
                            {
                                firstIndex = range.FirstIndex;
                                if (range.IsBaseLocator && range.FirstIndex > 0)
                                    firstIndex--;

                                lastIndex = range.LastIndex;
                            }
                            break;
                        default:
                            throw new NotSupportedException(operation.Scope.ToString());
                    }

                    for (int i = firstIndex; i <= lastIndex; i++)
                    {
                        TBranch branch = Branches[i].Value;

                        branch.ApplyToCache(locator, operation);

                        if (branch.NodeState != NodeState.None)
                            HaveChildrenForMaintenance = true;
                    }
                }

                IsModified = true;
            }

            public override TNode Split()
            {
                TBranch rightBranch = new TBranch(Branch.Tree, NodeType.Internal);
                TInternalNode rightNode = (TInternalNode)rightBranch.Node;

                int leftCount = 0;
                int leftBranchesCount = Branches.Count / 2;

                for (int i = 0; i < leftBranchesCount; i++)
                    leftCount += Branches[i].Value.Cache.OperationCount;

                rightNode.Branches.AddRange(Branches, leftBranchesCount, Branches.Count - leftBranchesCount);
                Branches.RemoveRange(leftBranchesCount, Branches.Count - leftBranchesCount);

                RebuildOptimizator();
                rightNode.RebuildOptimizator();

                rightNode.TouchID = TouchID;

                IsModified = true;

                return rightNode;
            }

            public override void Merge(TNode node)
            {
                TInternalNode rightNode = (TInternalNode)node;
                Branches.AddRange(rightNode.Branches);

                RebuildOptimizator();
                rightNode.RebuildOptimizator();

                if (TouchID < node.TouchID)
                    TouchID = node.TouchID;

                IsModified = true;
            }

            public void BroadcastFall(int level, TToken token, TParams param)
            {
                int firstIndex, lastIndex;
                if (param.IsTotal)
                {
                    firstIndex = 0;
                    lastIndex = Branches.Count - 1;
                }
                else
                {
                    TRange range = Optimizator.FindRange(param.Path);
                    if (param.IsPoint)
                    {
                        firstIndex = lastIndex = Optimizator.FindIndex(range, param.Path, param.FromKey);
                        Debug.Assert(firstIndex >= 0);
                    }
                    else if (param.IsOverall)
                    {
                        firstIndex = range.FirstIndex;
                        if (range.IsBaseLocator && range.FirstIndex > 0)
                            firstIndex--;

                        lastIndex = range.LastIndex;
                    }
                    else
                    {
                        firstIndex = Optimizator.FindIndex(range, param.Path, param.FromKey);
                        if (firstIndex < 0)
                            firstIndex = 0;
                        lastIndex = Optimizator.FindIndex(range, param.Path, param.ToKey);
                    }
                }

                IEnumerable<KeyValuePair<TFullKey, TBranch>> branches;
                switch (param.WalkMethod)
                {
                    case TWalkMethod.CascadeFirst:
                        branches = Branches.Range(firstIndex, firstIndex);
                        break;
                    case TWalkMethod.CascadeLast:
                        branches = Branches.Range(lastIndex, lastIndex);
                        break;
                    case TWalkMethod.Cascade:
                        branches = Branches.Range(firstIndex, lastIndex);
                        break;
                    case TWalkMethod.CascadeButOnlyLoaded:
                        branches = Branches.Range(firstIndex, lastIndex);
                        break;
                    default:
                        throw new NotSupportedException(param.WalkMethod.ToString());
                }

                TaskCreationOptions taskCreationOptions = TaskCreationOptions.None;
                //if ((param.TWalkAction & TWalkAction.Store) == TWTree<TPath>.TWalkAction.Store)
                  //  taskCreationOptions = TaskCreationOptions.AttachedToParent;

                Parallel.ForEach(branches, (branch) =>
                    {
                        if (param.WalkMethod == TWalkMethod.CascadeButOnlyLoaded)
                        {
                            branch.Value.WaitFall();
                            if (!branch.Value.IsNodeLoaded)
                                return;
                        }

                        if (branch.Value.Fall(level, token, param, taskCreationOptions))
                            IsModified = true;

                        //if ((param.TWalkAction & TWalkAction.Store) == TWTree<TPath>.TWalkAction.Store)
                        //    branch.Value.WaitFall();
                    });
            }

            public override bool IsOverflow
            {
                get { return Branches.Count > Branch.Tree.INTERNAL_NODE_MAX_BRANCHES; }
            }

            public override bool IsUnderflow
            {
                get
                {
                    if (IsRoot)
                        return Branches.Count < 2;

                    return Branches.Count < Branch.Tree.INTERNAL_NODE_MIN_BRANCHES;
                }
            }

            public override TFullKey FirstKey
            {
                get { return Branches[0].Key; }
            }

            public override void Store(Stream stream)
            {
                BinaryWriter writer = new BinaryWriter(stream);
                writer.Write(VERSION);

                TCountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

                writer.Write(HaveChildrenForMaintenance);
                Branches.Store(Branch.Tree, writer);

                IsModified = false;
            }

            public override void Load(Stream stream)
            {
                BinaryReader reader = new BinaryReader(stream);
                if (reader.ReadByte() != VERSION)
                    throw new Exception("Invalid TInternalNode version.");

                long id = (long)TCountCompression.Deserialize(reader);
                if (id != Branch.NodeHandle)
                    throw new Exception("Wtree logical error.");

                HaveChildrenForMaintenance = reader.ReadBoolean();
                Branches.Load(Branch.Tree, reader);

                RebuildOptimizator();

                IsModified = false;
            }

            private KeyValuePair<TFullKey, TBranch> FindFirstBranch(TRange range, ref TFullKey nearFullKey, ref bool hasNearFullKey)
            {
                int idx;
                if (!range.IsBaseLocator)
                    idx = range.LastIndex;
                else
                {
                    idx = range.FirstIndex;
                    if (idx > 0)
                        idx--;
                }

                if (idx + 1 < Branches.Count)
                {
                    hasNearFullKey = true;
                    nearFullKey = Branches[idx + 1].Key;
                }

                return Branches[idx]; 
            }

            private KeyValuePair<TFullKey, TBranch> FindLastBranch(TRange range, ref TFullKey nearFullKey, ref bool hasNearFullKey)
            {
                int idx = range.LastIndex; //no matter of range.IsBaseLocator

                if (idx > 0)
                {
                    hasNearFullKey = true;
                    nearFullKey = Branches[idx - 1].Key;
                }

                return Branches[idx]; 
            }

            public TBranch FindLastBranch(TLocator locator, ref TFullKey nearFullKey, ref bool hasNearFullKey)
            {
                TRange range = Optimizator.FindRange(locator);
                int idx = range.LastIndex; //no matter of range.IsBaseLocator

                if (idx > 0)
                {
                    hasNearFullKey = true;
                    nearFullKey = Branches[idx - 1].Key;
                }

                return Branches[idx].Value;
            }

            /// <summary>
            /// The hook.
            /// </summary>
            public KeyValuePair<TFullKey, TBranch> FindBranch(TLocator locator, ITData key, Direction direction, ref TFullKey nearFullKey, ref bool hasNearFullKey)
            {
                TRange range = Optimizator.FindRange(locator);
                
                if (key == null)
                {
                    switch (direction)
                    {
                        case Direction.Forward:
                            return FindFirstBranch(range, ref nearFullKey, ref hasNearFullKey);
                        case Direction.Backward:
                            return FindLastBranch(range, ref nearFullKey, ref hasNearFullKey);
                        default:
                            throw new NotSupportedException(direction.ToString());
                    }
                }

                int idx = Optimizator.FindIndex(range, locator, key);
                Debug.Assert(idx >= 0);

                switch (direction)
                {
                    case Direction.Backward:
                        {
                            if (idx > 0)
                            {
                                nearFullKey = Branches[idx - 1].Key;
                                hasNearFullKey = true;
                            }
                        }
                        break;
                    case Direction.Forward:
                        {
                            if (idx < Branches.Count - 1)
                            {
                                hasNearFullKey = true;
                                nearFullKey = Branches[idx + 1].Key;
                            }
                        }
                        break;
                }

                return Branches[idx];
            }

            public void RebuildOptimizator()
            {
                Optimizator.Rebuild(Branches);
            }
        }
    }
}