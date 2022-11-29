using System.Diagnostics;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private sealed partial class TInternalNode : TNode
        {
            public volatile bool HaveChildrenForMaintenance;

            public void Maintenance(int level, TToken token)
            {
                if (HaveChildrenForMaintenance)
                {
                    MaintenanceHelper[] helpers = new MaintenanceHelper[Branches.Count];
                    for (int index = Branches.Count - 1; index >= 0; index--)
                        helpers[index] = new MaintenanceHelper(level, token, helpers, Branches[index], index);

                    Branches.Clear();
                    for (int index = 0; index < helpers.Length; index++)
                    {
                        var helper = helpers[index];
                        helper.Task.Wait();
                        Branches.AddRange(helper.List);
                    }

                    RebuildOptimizator();

                    HaveChildrenForMaintenance = false;

                    IsModified = true;
                }

                //sink branches
                int operationCount = Branches.Sum(x => x.Value.Cache.OperationCount);
                if (operationCount > Branch.Tree.INTERNAL_NODE_MAX_OPERATIONS)
                {
                    //Debug.WriteLine(string.Format("{0}: {1} = {2}", level, TBranch.NodeHandle, operationCount));
                    foreach (var kv in Branches.Where(x => x.Value.Cache.OperationCount > 0).OrderByDescending(x => x.Value.Cache.OperationCount))
                    {
                        TBranch branch = kv.Value;

                        operationCount -= branch.Cache.OperationCount;
                        if (branch.Fall(level, token, new TParams(TWalkMethod.Current, TWalkAction.None, null, true)))
                            IsModified = true;

                        if (operationCount <= Branch.Tree.INTERNAL_NODE_MIN_OPERATIONS)
                            break;

                        //branch.WaitFall();
                    }
                }
            }

            private class MaintenanceHelper
            {
                public readonly int Level;
                public readonly TToken Token;
                public readonly MaintenanceHelper[] Helpers;
                public readonly int Index;

                public TBranchCollection List;
                public Task Task;

                public MaintenanceHelper(int level, TToken token, MaintenanceHelper[] helpers, KeyValuePair<TFullKey, TBranch> kv, int index)
                {
                    Level = level;
                    Token = token;
                    Helpers = helpers;
                    Index = index;
                    Task = Task.Factory.StartNew(Do, kv, TaskCreationOptions.AttachedToParent);
                }

                private void Split(int index)
                {
                    TNode node = List[index].Value.Node;
                    TBranch branch = node.Branch;

                    TNode rightNode = node.Split();
                    node.Branch.NodeState = node.State;
                    TBranch rightBranch = rightNode.Branch;

                    branch.NodeState = node.State;
                    rightBranch.NodeState = rightNode.State;

                    List.Insert(index + 1, new KeyValuePair<TFullKey, TBranch>(rightNode.FirstKey, rightBranch));
                    if (rightNode.IsOverflow)
                        Split(index + 1);
                    if (node.IsOverflow)
                        Split(index);
                }

                private void Merge(TNode node)
                {
                    TBranch branch = List[List.Count - 1].Value;
                    Debug.Assert(branch.Cache.OperationCount == 0);

                    branch.Node.Merge(node);
                    branch.NodeState = branch.Node.State;

                    //release node space
                    node.Branch.Tree.heap.Release(node.Branch.NodeHandle);
                }

                private void Do(object state)
                {
                    var kv = (KeyValuePair<TFullKey, TBranch>)state;
                    TBranch branch = kv.Value;

                    bool isFall = false;

                    branch.WaitFall();
                    if (branch.NodeState == NodeState.None)
                        List = new TBranchCollection(kv);
                    else
                    {
                        branch.Fall(Level, Token, new TParams(TWalkMethod.Current, TWalkAction.None, null, true));
                        branch.WaitFall();
                        isFall = true;

                        if (branch.NodeState == NodeState.None)
                            List = new TBranchCollection(kv);
                        else
                        {
                            List = new TBranchCollection();
                            List.Add(kv);

                            if (branch.NodeState == NodeState.Overflow)
                                Split(0);
                        }
                    }

                    if (Index + 1 >= Helpers.Length)
                        return;

                    var h = Helpers[Index + 1];
                    h.Task.Wait();

                    if (branch.NodeState == NodeState.Underflow || h.List[0].Value.NodeState == NodeState.Underflow)
                    {
                        if (!isFall)
                        {
                            branch.Fall(Level, Token, new TParams(TWalkMethod.Current, TWalkAction.None, null, true));
                            branch.WaitFall();
                        }

                        if (h.List[0].Value.Cache.OperationCount > 0)
                        {
                            h.List[0].Value.Fall(Level, Token, new TParams(TWalkMethod.Current, TWalkAction.None, null, true));
                            h.List[0].Value.WaitFall();
                        }

                        Debug.Assert(h.List[0].Value.Cache.OperationCount == 0);
                        Merge(h.List[0].Value.Node);
                        h.List.RemoveAt(0);
                    }

                    if (List[List.Count - 1].Value.NodeState == NodeState.Overflow)
                        Split(List.Count - 1);
                }
            }
        }
    }
}
