using FenySoft.Core.Data;
using FenySoft.Core.Compression;
using FenySoft.Core.Persist;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote
{
    public class TDescriptor : ITDescriptor
    {
        private TDescriptorStructure InternalDescriptor { get; set; }

        public TDescriptor(long id, string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType, DateTime createTime, DateTime modifiedTime, DateTime accessTime, byte[] tag)
        {
            InternalDescriptor = new TDescriptorStructure(id, name, structureType, keyDataType, recordDataType, keyType, recordType, createTime, modifiedTime, accessTime, tag);
        }

        public TDescriptor(long id, string name, TDataType keyDataType, TDataType recordDataType)
            : this(id, name, Database.TStructureType.XTABLE, keyDataType, recordDataType, TDataTypeUtils.BuildType(keyDataType), TDataTypeUtils.BuildType(recordDataType), DateTime.Now, DateTime.Now, DateTime.Now, null)
        {
        }

        public TDescriptor(long id, string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType)
            : this(id, name, structureType, keyDataType, recordDataType, keyType, recordType, DateTime.Now, DateTime.Now, DateTime.Now, null)
        {
        }

        private TDescriptor(TDescriptorStructure descriptor)
        {
            InternalDescriptor = descriptor;
        }

        #region ITDescriptor

        public long ID
        {
            get { return InternalDescriptor.ID; }
            set { InternalDescriptor.ID = value; }
        }

        public string Name
        {
            get { return InternalDescriptor.Name; }
            set { InternalDescriptor.Name = value; }
        }

        public int StructureType
        {
            get { return InternalDescriptor.StructureType; }
            set { InternalDescriptor.StructureType = value; }
        }

        public TDataType KeyDataType
        {
            get { return InternalDescriptor.KeyDataType; }
            set { InternalDescriptor.KeyDataType = value; }
        }

        public TDataType RecordDataType
        {
            get { return InternalDescriptor.RecordDataType; }
            set { InternalDescriptor.RecordDataType = value; }
        }

        public Type KeyType
        {
            get { return InternalDescriptor.KeyType; }
            set { InternalDescriptor.KeyType = value; }
        }

        public Type RecordType
        {
            get { return InternalDescriptor.RecordType; }
            set { InternalDescriptor.RecordType = value; }
        }

        public IComparer<ITData> KeyComparer
        {
            get { return InternalDescriptor.KeyComparer; }
            set { InternalDescriptor.KeyComparer = value; }
        }

        public IEqualityComparer<ITData> KeyEqualityComparer
        {
            get { return InternalDescriptor.KeyEqualityComparer; }
            set { InternalDescriptor.KeyEqualityComparer = value; }
        }

        public ITPersist<ITData> KeyPersist
        {
            get { return InternalDescriptor.KeyPersist; }
            set { InternalDescriptor.KeyPersist = value; }
        }

        public ITPersist<ITData> RecordPersist
        {
            get { return InternalDescriptor.RecordPersist; }
            set { InternalDescriptor.RecordPersist = value; }
        }

        public ITIndexerPersist<ITData> KeyIndexerPersist
        {
            get { return InternalDescriptor.KeyIndexerPersist; }
            set { InternalDescriptor.KeyIndexerPersist = value; }
        }

        public ITIndexerPersist<ITData> RecordIndexerPersist
        {
            get { return InternalDescriptor.RecordIndexerPersist; }
            set { InternalDescriptor.RecordIndexerPersist = value; }
        }

        public DateTime CreateTime
        {
            get { return InternalDescriptor.CreateTime; }
            private set { InternalDescriptor.CreateTime = value; }
        }

        public DateTime ModifiedTime
        {
            get { return InternalDescriptor.ModifiedTime; }
            private set { InternalDescriptor.ModifiedTime = value; }
        }

        public DateTime AccessTime
        {
            get { return InternalDescriptor.AccessTime; }
            private set { InternalDescriptor.AccessTime = value; }
        }

        public byte[] Tag
        {
            get { return InternalDescriptor.Tag; }
            set { InternalDescriptor.Tag = value; }
        }

        #endregion

        public void Serialize(BinaryWriter writer)
        {
            InternalDescriptor.Serialize(writer);
        }

        public static ITDescriptor Deserialize(BinaryReader reader)
        {
            TDescriptorStructure descriptorStructure = TDescriptorStructure.Deserialize(reader);

            return new TDescriptor(descriptorStructure);
        }
    }

    public class TDescriptorStructure : ITDescriptor
    {
        public TDescriptorStructure(long id, string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType, DateTime createTime, DateTime modifiedTime, DateTime accessTime, byte[] tag)
        {
            ID = id;
            Name = name;
            StructureType = structureType;

            KeyDataType = keyDataType;
            RecordDataType = recordDataType;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = createTime;
            ModifiedTime = modifiedTime;
            AccessTime = accessTime;

            Tag = tag;
        }

        #region ITDescriptor

        public long ID { get; set; }
        public string Name { get; set; }
        public int StructureType { get; set; }

        public TDataType KeyDataType { get; set; }
        public TDataType RecordDataType { get; set; }

        public Type KeyType { get; set; }
        public Type RecordType { get; set; }

        public IComparer<ITData> KeyComparer { get; set; }
        public IEqualityComparer<ITData> KeyEqualityComparer { get; set; }

        public ITPersist<ITData> KeyPersist { get; set; }
        public ITPersist<ITData> RecordPersist { get; set; }

        public ITIndexerPersist<ITData> KeyIndexerPersist { get; set; }
        public ITIndexerPersist<ITData> RecordIndexerPersist { get; set; }

        public DateTime CreateTime { get; set; }
        public DateTime ModifiedTime { get; set; }
        public DateTime AccessTime { get; set; }

        public byte[] Tag { get; set; }

        //public byte[] Tag
        //{
        //    get
        //    {
        //        //if (ForSerialize)
        //        //    return tag;

        //        ITDescriptor descriptor = GetDescriptor(this);
        //        tag = descriptor.Tag;

        //        return tag;
        //    }
        //    set
        //    {
        //        //this.ForSerialize = true;
        //        //this.tag = value;

        //        //ITDescriptor descriptor = SetDescriptor(this);

        //        //tag = descriptor.Tag;
        //        //this.ForSerialize = false;
        //    }
        //}

        #endregion

        public void Serialize(BinaryWriter writer)
        {
            TCountCompression.Serialize(writer, (ulong)ID);
            writer.Write(Name);

            TCountCompression.Serialize(writer, (ulong)StructureType);

            KeyDataType.Serialize(writer);
            RecordDataType.Serialize(writer);

            TCountCompression.Serialize(writer, (ulong)CreateTime.Ticks);
            TCountCompression.Serialize(writer, (ulong)ModifiedTime.Ticks);
            TCountCompression.Serialize(writer, (ulong)AccessTime.Ticks);

            if (Tag == null)
                TCountCompression.Serialize(writer, 0);
            else
            {
                TCountCompression.Serialize(writer, (ulong)Tag.Length + 1);
                writer.Write(Tag);
            }
        }

        public static TDescriptorStructure Deserialize(BinaryReader reader)
        {
            long id = (long)TCountCompression.Deserialize(reader);
            string name = reader.ReadString();

            int structureType = (int)TCountCompression.Deserialize(reader);

            var keyDataType = TDataType.Deserialize(reader);
            var recordDataType = TDataType.Deserialize(reader);

            var keyType = TDataTypeUtils.BuildType(keyDataType);
            var recordType = TDataTypeUtils.BuildType(recordDataType);

            var createTime = new DateTime((long)TCountCompression.Deserialize(reader));
            var modifiedTime = new DateTime((long)TCountCompression.Deserialize(reader));
            var accessTime = new DateTime((long)TCountCompression.Deserialize(reader));

            var tagLength = (int)TCountCompression.Deserialize(reader) - 1;
            byte[] tag = tagLength >= 0 ? reader.ReadBytes(tagLength) : null;

            return new TDescriptorStructure(id, name, structureType, keyDataType, recordDataType, keyType, recordType, createTime, modifiedTime, accessTime, tag);
        }
    }
}