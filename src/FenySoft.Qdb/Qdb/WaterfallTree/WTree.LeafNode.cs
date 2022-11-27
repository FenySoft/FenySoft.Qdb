using System.Diagnostics;

using FenySoft.Core.Compression;
using FenySoft.Core.Collections;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class WTree
    {
        private sealed class LeafNode : Node
        {
            public const byte VERSION = 40;

            /// <summary>
            /// Total number of records in the node
            /// </summary>
            public int RecordCount { get; private set; }

            public readonly Dictionary<Locator, IOrderedSet<ITData, ITData>> Container;

            public LeafNode(Branch branch, bool isModified)
                : base(branch)
            {
                Debug.Assert(branch.NodeType == NodeType.Leaf);

                Container = new Dictionary<Locator, IOrderedSet<ITData, ITData>>();
                IsModified = isModified;
            }

            public override void Apply(IOperationCollection operations)
            {
                Locator locator = operations.Locator;

                IOrderedSet<ITData, ITData> data;
                if (Container.TryGetValue(locator, out data))
                {
                    RecordCount -= data.Count;

                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count == 0)
                        Container.Remove(locator);
                }
                else
                {
                    data = locator.OrderedSetFactory.Create();
                    Debug.Assert(data != null);
                    if (locator.Apply.Leaf(operations, data))
                        IsModified = true;

                    RecordCount += data.Count;

                    if (data.Count > 0)
                        Container.Add(locator, data);
                }
            }

            public override Node Split()
            {
                int HALF_RECORD_COUNT = RecordCount / 2;

                Branch rightBranch = new Branch(Branch.Tree, NodeType.Leaf);
                LeafNode rightNode = ((LeafNode)rightBranch.Node);
                var rightContainer = rightNode.Container;

                int leftRecordCount = 0;

                KeyValuePair<Locator, IOrderedSet<ITData, ITData>> specialCase = new KeyValuePair<Locator, IOrderedSet<ITData, ITData>>(default(Locator), null);

                if (Container.Count == 1)
                {
                    var kv = Container.First();
                    var data = kv.Value.Split(HALF_RECORD_COUNT);

                    Debug.Assert(data.Count > 0);
                    rightContainer.Add(kv.Key, data);
                    leftRecordCount = RecordCount - data.Count;
                }
                else //if (Container.Count > 1)
                {
                    var enumerator = Container.OrderBy(x => x.Key).GetEnumerator();

                    List<Locator> emptyContainers = new List<Locator>();

                    //the left part
                    while (enumerator.MoveNext())
                    {
                        var kv = enumerator.Current;
                        if (kv.Value.Count == 0)
                        {
                            emptyContainers.Add(kv.Key);
                            continue;
                        }

                        leftRecordCount += kv.Value.Count;
                        if (leftRecordCount < HALF_RECORD_COUNT)
                            continue;

                        if (leftRecordCount > HALF_RECORD_COUNT)
                        {
                            var data = kv.Value.Split(leftRecordCount - HALF_RECORD_COUNT);
                            if (data.Count > 0)
                            {
                                specialCase = new KeyValuePair<Locator, IOrderedSet<ITData, ITData>>(kv.Key, data);
                                leftRecordCount -= data.Count;
                            }
                        }

                        break;
                    }

                    //the right part
                    while (enumerator.MoveNext())
                    {
                        var kv = enumerator.Current;
                        if (kv.Value.Count == 0)
                        {
                            emptyContainers.Add(kv.Key);
                            continue;
                        }

                        rightContainer[kv.Key] = kv.Value;
                    }

                    foreach (var kv in rightContainer)
                        Container.Remove(kv.Key);

                    foreach (var key in emptyContainers)
                        Container.Remove(key);

                    if (specialCase.Value != null) //have special case?
                        rightContainer[specialCase.Key] = specialCase.Value;
                }

                rightNode.RecordCount = RecordCount - leftRecordCount;
                RecordCount = leftRecordCount;
                rightNode.TouchID = TouchID;
                IsModified = true;

                return rightNode;
            }

            public override void Merge(Node node)
            {
                foreach (var kv in ((LeafNode)node).Container)
                {
                    IOrderedSet<ITData, ITData> data;
                    if (!Container.TryGetValue(kv.Key, out data))
                        Container[kv.Key] = data = kv.Value;
                    else
                    {
                        RecordCount -= data.Count;
                        data.Merge(kv.Value);
                    }

                    RecordCount += data.Count;
                }

                if (TouchID < node.TouchID)
                    TouchID = node.TouchID;

                IsModified = true;
            }

            public override bool IsOverflow
            {
                get { return RecordCount > Branch.Tree.LEAF_NODE_MAX_RECORDS; }
            }

            public override bool IsUnderflow
            {
                get
                {
                    if (IsRoot)
                        return false;

                    return RecordCount < Branch.Tree.LEAF_NODE_MIN_RECORDS;
                }
            }

            public override FullKey FirstKey
            {
                get
                {
                    var kv = (Container.Count == 1) ? Container.First() : Container.OrderBy(x => x.Key).First();

                    return new FullKey(kv.Key, kv.Value.First.Key);
                }
            }

            public override void Store(Stream stream)
            {
                BinaryWriter writer = new BinaryWriter(stream);
                writer.Write(VERSION);

                CountCompression.Serialize(writer, checked((ulong)Branch.NodeHandle));

                CountCompression.Serialize(writer, checked((ulong)Container.Count));
                foreach (var kv in Container)
                {
                    Branch.Tree.SerializeLocator(writer, kv.Key);
                    kv.Key.OrderedSetPersist.Write(writer, kv.Value);
                }

                IsModified = false;
            }

            public override void Load(Stream stream)
            {
                BinaryReader reader = new BinaryReader(stream);
                if (reader.ReadByte() != VERSION)
                    throw new Exception("Invalid LeafNode version.");

                long id = (long)CountCompression.Deserialize(reader);
                if (id != Branch.NodeHandle)
                    throw new Exception("Wtree logical error.");

                int count = (int)CountCompression.Deserialize(reader);
                for (int i = 0; i < count; i++)
                {
                    Locator path = Branch.Tree.DeserializeLocator(reader);
                    IOrderedSet<ITData, ITData> data = path.OrderedSetPersist.Read(reader);
                    Container[path] = data;

                    RecordCount += data.Count;
                }

                IsModified = false;
            }

            public IOrderedSet<ITData, ITData> FindData(Locator locator, Direction direction, ref FullKey nearFullKey, ref bool hasNearFullKey)
            {
                IOrderedSet<ITData, ITData> data = null;
                Container.TryGetValue(locator, out data);
                if (direction == Direction.None)
                    return data;

                if (Container.Count == 1 && data != null)
                    return data;

                IOrderedSet<ITData, ITData> nearData = null;
                if (direction == Direction.Backward)
                {
                    bool havePrev = false;
                    Locator prev = default(Locator);

                    foreach (var kv in Container)
                    {
                        if (kv.Key.CompareTo(locator) < 0)
                        {
                            if (!havePrev || kv.Key.CompareTo(prev) > 0)
                            {
                                prev = kv.Key;
                                nearData = kv.Value;
                                havePrev = true;
                            }
                        }
                    }

                    if (havePrev)
                    {
                        hasNearFullKey = true;
                        nearFullKey = new FullKey(prev, nearData.Last.Key);
                    }
                }
                else //if (direction == Direction.Forward)
                {
                    bool haveNext = false;
                    Locator next = default(Locator);

                    foreach (var kv in Container)
                    {
                        if (kv.Key.CompareTo(locator) > 0)
                        {
                            if (!haveNext || kv.Key.CompareTo(next) < 0)
                            {
                                next = kv.Key;
                                nearData = kv.Value;
                                haveNext = true;
                            }
                        }
                    }

                    if (haveNext)
                    {
                        hasNearFullKey = true;
                        nearFullKey = new FullKey(next, nearData.First.Key);
                    }
                }

                return data;
            }
        }
    }
}
