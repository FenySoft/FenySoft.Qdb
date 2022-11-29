using System.Diagnostics;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private abstract class TNode
        {
            public bool IsModified { get; protected set; }
            public TBranch Branch;
            public volatile bool IsExpiredFromCache;
#if DEBUG
            public volatile int TaskID;
#endif
            private static long globalTouchID = 0;
            private long touchID;

            public long TouchID
            {
                get { return Interlocked.Read(ref touchID); }
                set { Interlocked.Exchange(ref touchID, value); }
            }

            public TNode(TBranch branch)
            {
                Branch = branch;
            }

            public abstract void Apply(ITOperationCollection operations);
            public abstract TNode Split();
            public abstract void Merge(TNode node);
            public abstract bool IsOverflow { get; }
            public abstract bool IsUnderflow { get; }
            public abstract TFullKey FirstKey { get; }

            public abstract void Store(Stream stream);
            public abstract void Load(Stream stream);

            public void Touch(long count)
            {
                Debug.Assert(count > 0);
                touchID = Interlocked.Add(ref globalTouchID, count);

                //IsExpiredFromCache = false;
            }

            //only for speed reason
            public NodeType Type
            {
                get { return Branch.NodeType; }
            }

            public bool IsRoot
            {
                get { return ReferenceEquals(Branch.Tree.RootBranch, Branch); }
            }

            public NodeState State
            {
                get
                {
                    if (IsOverflow)
                        return NodeState.Overflow;

                    if (IsUnderflow)
                        return NodeState.Underflow;

                    return NodeState.None;
                }
            }

            public void Store()
            {
                using (MemoryStream stream = new MemoryStream())
                {
                    Store(stream);

                    //int recordCount = 0;
                    //string type = "";
                    //if (this is TInternalNode)
                    //{
                    //    recordCount = ((TInternalNode)this).TBranch.TCache.OperationCount;
                    //    type = "Internal";
                    //}
                    //else
                    //{
                    //    recordCount = ((TLeafNode)this).RecordCount;
                    //    type = "Leaf";
                    //}
                    //double sizeInMB = Math.Round(stream.Length / (1024.0 * 1024), 2);
                    //Console.WriteLine("{0} {1}, Records {2}, Size {3} MB", type, TBranch.NodeHandle, recordCount, sizeInMB);

                    Branch.Tree.heap.Write(Branch.NodeHandle, stream.GetBuffer(), 0, (int)stream.Length);
                }
            }

            public void Load()
            {
                var heap = Branch.Tree.heap;
                byte[] buffer = heap.Read(Branch.NodeHandle);
                Load(new MemoryStream(buffer));
            }

            public static TNode Create(TBranch branch)
            {
                TNode node;
                switch (branch.NodeType)
                {
                    case NodeType.Leaf:
                        node = new TLeafNode(branch, true);
                        break;
                    case NodeType.Internal:
                        node = new TInternalNode(branch, new TBranchCollection(), true);
                        break;
                    default:
                        throw new NotSupportedException();
                }

                branch.Tree.Packet(node.Branch.NodeHandle, node);
                return node;
            }
        }

        public enum NodeState
        {
            None,
            Overflow,
            Underflow
        }

        protected enum NodeType : byte
        {
            Leaf,
            Internal
        }
    }
}
