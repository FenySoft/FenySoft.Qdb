using FenySoft.Core.Data;
using FenySoft.Core.Collections;
using FenySoft.Core.Compression;
using FenySoft.Core.Persist;

using System.Diagnostics;

using FenySoft.Qdb.Database;

namespace FenySoft.Qdb.WaterfallTree
{
    public class TLocator : ITDescriptor, IComparable<TLocator>, IEquatable<TLocator>
    {
        public const byte VERSION = 40;

        private byte[] serializationData;

        private int hashCode;

        private bool isDeleted;

        private string name;

        private Type keyType;
        private Type recordType;

        private Dictionary<string, int> keyMembers;
        private Dictionary<string, int> recordMembers;

        private IComparer<ITData> keyComparer;
        private IEqualityComparer<ITData> keyEqualityComparer;
        private ITPersist<ITData> keyPersist;
        private ITPersist<ITData> recordPersist;
        private ITIndexerPersist<ITData> keyIndexerPersist;
        private ITIndexerPersist<ITData> recordIndexerPersist;

        private DateTime createTime;
        private DateTime modifiedTime;
        private DateTime accessTime;

        private byte[] tag;

        public readonly object SyncRoot = new object();

        internal static readonly TLocator MIN;

        static TLocator()
        {
            MIN = new TLocator(0, null, Database.TStructureType.RESERVED, TDataType.Boolean, TDataType.Boolean, null, null);
            MIN.keyPersist = TSentinelPersistKey.Instance;
        }

        public ITOperationCollectionFactory OperationCollectionFactory;
        public ITOrderedSetFactory OrderedSetFactory;

        public TLocator(long id, string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType)
        {
            if (keyDataType == null)
                throw new ArgumentException("keyDataType");
            if (recordDataType == null)
                throw new ArgumentException("recordDataType");

            ID = id;
            Name = name;
            StructureType = structureType;

            hashCode = ID.GetHashCode();

            //apply
            switch (structureType)
            {
                case Database.TStructureType.XTABLE: Apply = new TXTableApply(this); break;
                case Database.TStructureType.XFILE:  Apply = new TXStreamApply(this); break;
            }

            KeyDataType = keyDataType;
            RecordDataType = recordDataType;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = DateTime.Now;
            ModifiedTime = CreateTime;
            AccessTime = CreateTime;

            OperationCollectionFactory = new TOperationCollectionFactory(this);
            OrderedSetFactory = new TOrderedSetFactory(this);
        }

        private void WriteMembers(BinaryWriter writer, Dictionary<string, int> members)
        {
            if (members == null)
            {
                writer.Write(false);
                return;
            }

            writer.Write(true);
            writer.Write(members.Count);

            foreach (var kv in members)
            {
                writer.Write(kv.Key);
                writer.Write(kv.Value);
            }
        }

        private static Dictionary<string, int> ReadMembers(BinaryReader reader)
        {
            if (!reader.ReadBoolean())
                return null;

            int count = reader.ReadInt32();
            Dictionary<string, int> members = new Dictionary<string, int>(count);

            for (int i = 0; i < count; i++)
            {
                string key = reader.ReadString();
                int value = reader.ReadInt32();

                members.Add(key, value);
            }

            return members;
        }

        private void InternalSerialize(BinaryWriter writer)
        {
            lock (SyncRoot)
            {
                writer.Write(VERSION);

                writer.Write(ID);
                if (ID == MIN.ID)
                    return;

                writer.Write(IsDeleted);

                writer.Write(Name);
                writer.Write(checked((byte)StructureType));

                //data types
                KeyDataType.Serialize(writer);
                RecordDataType.Serialize(writer);

                //types
                if (!TDataTypeUtils.IsAnonymousType(KeyType))
                    writer.Write(KeyType.FullName);
                else
                    writer.Write("");

                if (!TDataTypeUtils.IsAnonymousType(RecordType))
                    writer.Write(RecordType.FullName);
                else
                    writer.Write("");

                //key & record members
                WriteMembers(writer, keyMembers);
                WriteMembers(writer, recordMembers);

                //times
                writer.Write(CreateTime.Ticks);
                writer.Write(ModifiedTime.Ticks);
                writer.Write(AccessTime.Ticks);

                //tag
                if (Tag == null)
                    writer.Write(false);
                else
                {
                    writer.Write(true);
                    TCountCompression.Serialize(writer, (ulong)Tag.Length);
                    writer.Write(Tag);
                }
            }
        }

        public void Serialize(BinaryWriter writer)
        {
            lock (SyncRoot)
            {
                if (serializationData == null)
                {
                    using (MemoryStream ms = new MemoryStream())
                    {
                        InternalSerialize(new BinaryWriter(ms));
                        serializationData = ms.ToArray();
                    }
                }

                writer.Write(serializationData);
            }
        }

        public static TLocator Deserialize(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid TLocator version.");

            long id = reader.ReadInt64();
            if (id == MIN.ID)
                return MIN;

            bool isDeleted = reader.ReadBoolean();

            string name = reader.ReadString();
            int structureType = reader.ReadByte();

            //data types
            TDataType keyDataType = TDataType.Deserialize(reader);
            TDataType recordDataType = TDataType.Deserialize(reader);

            //types
            string sKeyType = reader.ReadString();
            Type keyType = (sKeyType != "") ? TTypeCache.GetType(sKeyType) : TDataTypeUtils.BuildType(keyDataType);

            string sRecordType = reader.ReadString();
            Type recordType = (sRecordType != "") ? TTypeCache.GetType(sRecordType) : TDataTypeUtils.BuildType(recordDataType);

            //key & record members
            var keyMembers = ReadMembers(reader);
            var recordMembers = ReadMembers(reader);

            //create time
            DateTime createTime = new DateTime(reader.ReadInt64());
            DateTime modifiedTime = new DateTime(reader.ReadInt64());
            DateTime accessTime = new DateTime(reader.ReadInt64());

            //tag
            byte[] tag = reader.ReadBoolean() ? reader.ReadBytes((int)TCountCompression.Deserialize(reader)) : null;

            var locator = new TLocator(id, name, structureType, keyDataType, recordDataType, keyType, recordType);

            locator.IsDeleted = isDeleted;

            locator.keyMembers = keyMembers;
            locator.recordMembers = recordMembers;

            locator.CreateTime = createTime;
            locator.ModifiedTime = modifiedTime;
            locator.AccessTime = accessTime;

            locator.Tag = tag;

            return locator;
        }

        public bool IsReady { get; private set; }

        private TTypeEngine keyEngine;
        private TTypeEngine recEngine;

        private void DoPrepare()
        {
            Debug.Assert(KeyType != null);
            Debug.Assert(RecordType != null);

            //keys
            if (KeyComparer == null || KeyEqualityComparer == null || (KeyPersist == null || KeyIndexerPersist == null))
            {
                if (keyEngine == null)
                    keyEngine = TTypeEngine.Default(KeyType);

                if (KeyComparer == null)
                    KeyComparer = keyEngine.Comparer;

                if (KeyEqualityComparer == null)
                    KeyEqualityComparer = keyEngine.EqualityComparer;

                if (KeyPersist == null)
                    KeyPersist = keyEngine.Persist;

                if (KeyIndexerPersist == null)
                    KeyIndexerPersist = keyEngine.IndexerPersist;
            }

            //records
            if (RecordPersist == null || RecordIndexerPersist == null)
            {
                if (recEngine == null)
                    recEngine = TTypeEngine.Default(RecordType);

                if (RecordPersist == null)
                    RecordPersist = recEngine.Persist;

                if (RecordIndexerPersist == null)
                    RecordIndexerPersist = recEngine.IndexerPersist;
            }

            //container
            if (OrderedSetPersist == null)
            {
                if (KeyIndexerPersist != null && RecordIndexerPersist != null)
                    OrderedSetPersist = new TOrderedSetPersist(KeyIndexerPersist, RecordIndexerPersist, OrderedSetFactory);
                else
                    OrderedSetPersist = new TOrderedSetPersist(KeyPersist, RecordPersist, OrderedSetFactory);
            }

            //operations
            if (OperationsPersist == null)
                OperationsPersist = new TOperationCollectionPersist(KeyPersist, RecordPersist, OperationCollectionFactory);

            IsReady = true;
        }

        public ITApply Apply { get; private set; }
        public ITPersist<ITOrderedSet<ITData, ITData>> OrderedSetPersist { get; private set; }
        public ITPersist<ITOperationCollection> OperationsPersist { get; private set; }

        public int CompareTo(TLocator other)
        {
            return ID.CompareTo(other.ID);
        }

        public bool Equals(TLocator other)
        {
            return ID == other.ID;
        }

        public override bool Equals(object obj)
        {
            if (!(obj is TLocator))
                return false;

            return Equals((TLocator)obj);
        }

        public override int GetHashCode()
        {
            return hashCode;
        }

        public override string ToString()
        {
            return Name;
        }

        public static bool operator ==(TLocator x, TLocator y)
        {
            bool xNotNull = !ReferenceEquals(x, null);
            bool yNotNull = !ReferenceEquals(y, null);

            if (xNotNull && yNotNull)
                return x.Equals(y);

            if (xNotNull || yNotNull)
                return false;

            return true;
        }

        public static bool operator !=(TLocator x, TLocator y)
        {
            return !(x == y);
        }

        public static implicit operator string(TLocator locator)
        {
            return locator.ToString();
        }

        public bool IsDeleted
        {
            get
            {
                lock (SyncRoot)
                    return isDeleted;
            }

            set
            {
                lock (SyncRoot)
                {
                    if (value != isDeleted)
                    {
                        isDeleted = value;
                        serializationData = null;
                    }
                }
            }
        }

        #region IDescription

        public long ID { get; private set; }

        public string Name
        {
            get { return name; }
            set
            {
                lock (SyncRoot)
                {
                    name = value;
                    serializationData = null;
                }
            }
        }

        public int StructureType { get; private set; }

        public TDataType KeyDataType { get; private set; }
        public TDataType RecordDataType { get; private set; }

        public Type KeyType
        {
            get { return keyType; }
            set
            {
                if (keyType == value)
                    return;

                if (value != null && TDataTypeUtils.BuildDataType(value) != KeyDataType)
                    throw new Exception(String.Format("The type {0} is not compatible with anonymous type {1}.", value, KeyDataType));
                
                keyType = value;
                keyEngine = null;

                keyComparer = null;
                keyEqualityComparer = null;
                keyPersist = null;
                keyIndexerPersist = null;

                OrderedSetPersist = null;
                OperationsPersist = null;

                IsReady = false;
            }
        }

        public Type RecordType
        {
            get { return recordType; }
            set
            {
                if (recordType == value)
                    return;

                if (value != null && TDataTypeUtils.BuildDataType(value) != RecordDataType)
                    throw new Exception(String.Format("The type {0} is not compatible with anonymous type {1}.", value, RecordDataType));

                recordType = value;
                recEngine = null;

                recordPersist = null;
                recordIndexerPersist = null;

                OrderedSetPersist = null;
                OperationsPersist = null;

                IsReady = false;
            }
        }

        public IComparer<ITData> KeyComparer
        {
            get
            { 
                lock (SyncRoot)
                    return keyComparer; 
            }
            set
            {
                lock (SyncRoot)
                {
                    keyComparer = value;
                    IsReady = false;
                }
            }
        }

        public IEqualityComparer<ITData> KeyEqualityComparer
        {
            get
            { 
                lock (SyncRoot)
                    return keyEqualityComparer; 
            }
            set
            {
                lock (SyncRoot)
                {
                    keyEqualityComparer = value;
                    IsReady = false;
                }
            }
        }

        public ITPersist<ITData> KeyPersist
        {
            get
            {
                lock (SyncRoot)
                    return keyPersist; 
            }
            set
            {
                lock (SyncRoot)
                {
                    keyPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public ITPersist<ITData> RecordPersist
        {
            get
            { 
                lock (SyncRoot)
                    return recordPersist; 
            }
            set
            {
                lock (SyncRoot)
                {
                    recordPersist = value;

                    OrderedSetPersist = null;
                    OperationsPersist = null;

                    IsReady = false;
                }
            }
        }

        public ITIndexerPersist<ITData> KeyIndexerPersist
        {
            get
            { 
                lock (SyncRoot)
                    return keyIndexerPersist; 
            }
            set
            {
                lock (SyncRoot)
                {
                    keyIndexerPersist = value;
                    OrderedSetPersist = null;

                    IsReady = false;
                }
            }
        }

        public ITIndexerPersist<ITData> RecordIndexerPersist
        {
            get
            {
                lock (SyncRoot)
                    return recordIndexerPersist; 
            }
            set
            {
                lock (SyncRoot)
                {
                    recordIndexerPersist = value;
                    OrderedSetPersist = null;

                    IsReady = false;
                }
            }
        }

        public void Prepare()
        {
            if (!IsReady)
            {
                lock (SyncRoot)
                    DoPrepare();
            }
        }

        public DateTime CreateTime
        {
            get
            {
                lock (SyncRoot)
                    return createTime;
            }
            set
            {
                lock (SyncRoot)
                {
                    createTime = value;
                    serializationData = null;
                }
            }
        }

        public DateTime ModifiedTime
        {
            get
            {
                lock (SyncRoot)
                    return modifiedTime;
            }
            set
            {
                lock (SyncRoot)
                {
                    modifiedTime = value;
                    serializationData = null;
                }
            }
        }

        public DateTime AccessTime
        {
            get
            {
                lock (SyncRoot)
                    return accessTime;
            }
            set
            {
                lock (SyncRoot)
                {
                    accessTime = value;
                    serializationData = null;
                }
            }
        }

        public byte[] Tag
        {
            get
            { 
                lock (SyncRoot)
                    return tag; 
            }
            set
            {
                lock (SyncRoot)
                {
                    tag = value;
                    serializationData = null;
                }
            }
        }

        #endregion
    }
}
