namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        private partial class TBranch
        {
            public readonly TWTree Tree;
            public TBranchCache Cache = new TBranchCache();

            /// <summary>
            /// on load
            /// </summary>
            public TBranch(TWTree tree, NodeType nodeType, long nodeHandle)
            {
                Tree = tree;
                NodeType = nodeType;
                NodeHandle = nodeHandle;
            }

            /// <summary>
            /// on brand new branch
            /// </summary>
            public TBranch(TWTree tree, NodeType nodeType)
                : this(tree, nodeType, tree.heap.ObtainNewHandle())
            {
                node = TNode.Create(this);
            }

            public override string ToString()
            {
                return String.Format("NodeType = {0}, Handle = {1}, IsNodeLoaded = {2}, TCache.OperationCount = {3}", NodeType, NodeHandle, IsNodeLoaded, Cache.OperationCount);
            }

            #region TNode

            public NodeType NodeType;

            /// <summary>
            /// permanent and unique node handle 
            /// </summary>
            public long NodeHandle { get; set; }
            
            public volatile NodeState NodeState;

            public bool IsNodeLoaded
            {
                get { return node != null; }
            }

            private TNode node;

            public TNode Node
            {
                get
                {
                    if (node != null)
                        return node;

                    node = Tree.Retrieve(NodeHandle);

                    if (node != null)
                    {
                        node.Branch.WaitFall();
                        node.Branch = this;
                        Tree.Packet(NodeHandle, node);
                    }
                    else
                    {
                        node = TNode.Create(this);
                        node.Load();
                    }

                    return node;
                }
                set
                {
                    node = value;
                }
            }

            #endregion
        }
    }
}