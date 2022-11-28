using FenySoft.Core.Persist;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    //[Flags]
    //public enum CustomData : byte
    //{
    //    None = 0,
    //    KeyType = 1,
    //    RecordType = 2,
    //    KeyComparer = 4,
    //    KeyEqualityComparer = 8,
    //    KeyPersist = 16,
    //    RecordPersist = 32,
    //    KeyIndexerPersist = 64,
    //    RecordIndexerPersist = 128,

    //    All = KeyType | RecordType | KeyComparer | KeyEqualityComparer | KeyPersist | RecordPersist | KeyIndexerPersist | RecordIndexerPersist
    //}

    public interface ITDescriptor
    {
        long ID { get; }
        string Name { get; }
        int StructureType { get; }

        /// <summary>
        /// Describes the KeyType
        /// </summary>
        TDataType KeyDataType { get; }

        /// <summary>
        /// Describes the RecordType
        /// </summary>
        TDataType RecordDataType { get; }

        /// <summary>
        /// Can be anonymous or user type
        /// </summary>
        Type KeyType { get; set; }

        /// <summary>
        /// Can be anonymous or user type
        /// </summary>
        Type RecordType { get; set; }

        IComparer<ITData> KeyComparer { get; set; }
        IEqualityComparer<ITData> KeyEqualityComparer { get; set; }
        ITPersist<ITData> KeyPersist { get; set; }
        ITPersist<ITData> RecordPersist { get; set; }
        ITIndexerPersist<ITData> KeyIndexerPersist { get; set; }
        ITIndexerPersist<ITData> RecordIndexerPersist { get; set; }

        DateTime CreateTime { get; }
        DateTime ModifiedTime { get; }
        DateTime AccessTime { get; }

        byte[] Tag { get; set; }
    }
}