using System.Diagnostics;

using FenySoft.Core.Compression;
using FenySoft.Core.Comparers;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        [DebuggerDisplay("Count = {Count}")]
        private class TBranchCollection : List<KeyValuePair<TFullKey, TBranch>>
        {
            public static readonly TKeyValuePairComparer<TFullKey, TBranch> Comparer = new TKeyValuePairComparer<TFullKey, TBranch>(Comparer<TFullKey>.Default);

            public TBranchCollection()
            {
            }

            public TBranchCollection(int capacity)
                : base(capacity)
            {
            }


            public TBranchCollection(params KeyValuePair<TFullKey, TBranch>[] array)
                : base(array)
            {
            }

            public TBranchCollection(IEnumerable<KeyValuePair<TFullKey, TBranch>> collection)
                : base(collection)
            {
            }

            public int BinarySearch(TFullKey locator, int index, int length, IComparer<KeyValuePair<TFullKey, TBranch>> comparer)
            {
                return BinarySearch(index, length, new KeyValuePair<TFullKey, TBranch>(locator, null), comparer);
            }

            public int BinarySearch(TFullKey locator, int index, int length)
            {
                return BinarySearch(locator, index, length, Comparer);
            }

            public int BinarySearch(TFullKey locator)
            {
                return BinarySearch(locator, 0, Count);
            }

            public void Add(TFullKey locator, TBranch branch)
            {
                Add(new KeyValuePair<TFullKey, TBranch>(locator, branch));
            }

            public IEnumerable<KeyValuePair<TFullKey, TBranch>> Range(int fromIndex, int toIndex)
            {
                for (int i = fromIndex; i <= toIndex; i++)
                    yield return this[i];
            }

            public void Store(TWTree tree, BinaryWriter writer)
            {
                TCountCompression.Serialize(writer, checked((ulong)Count));

                Debug.Assert(Count > 0);
                writer.Write((byte)this[0].Value.NodeType);

                for (int i = 0; i < Count; i++)
                {
                    var kv = this[i];
                    TFullKey fullkey = kv.Key;
                    TBranch branch = kv.Value;
                    //lock (branch)
                    //{
                    //}

                    //write locator
                    tree.SerializeLocator(writer, fullkey.Locator);
                    fullkey.Locator.KeyPersist.Write(writer, fullkey.Key);

                    //write branch info
                    writer.Write(branch.NodeHandle);                    
                    writer.Write((int)branch.NodeState);
                    
                    branch.Cache.Store(tree, writer);                    
                }
            }

            public void Load(TWTree tree, BinaryReader reader)
            {
                int count = (int)TCountCompression.Deserialize(reader);
                Capacity = count;

                NodeType nodeType = (NodeType)reader.ReadByte();

                for (int i = 0; i < count; i++)
                {
                    //read fullKey
                    var locator = tree.DeserializeLocator(reader);
                    var key = locator.KeyPersist.Read(reader);
                    var fullKey = new TFullKey(locator, key);

                    //read branch info
                    long nodeID = reader.ReadInt64();
                    NodeState nodeState = (NodeState)reader.ReadInt32();

                    TBranch branch = new TBranch(tree, nodeType, nodeID);
                    branch.NodeState = nodeState;

                    branch.Cache.Load(tree, reader);

                    Add(new KeyValuePair<TFullKey, TBranch>(fullKey, branch));
                }
            }
        }
    }
}
