using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public interface ITStorageEngine : IEnumerable<ITDescriptor>, IDisposable
    {
        /// <summary>
        /// Works with anonymous types.
        /// </summary>
        ITTable<ITData, ITData> OpenXTablePortable(string AName, TDataType AKeyDataType, TDataType ARecordDataType);

        /// <summary>
        /// Works with portable types via custom transformers.
        /// </summary>
        ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName, TDataType AKeyDataType, TDataType ARecordDataType, ITransformer<TKey, ITData> AKeyTransformer, ITransformer<TRecord, ITData> ARecordTransformer);

        /// <summary>
        /// Works with anonymous types via default transformers.
        /// </summary>
        ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName);

        /// <summary>
        /// Works with the user types directly.
        /// </summary>
        ITTable<TKey, TRecord> OpenXTable<TKey, TRecord>(string AName);

        /// <summary>
        /// 
        /// </summary>
        XFile OpenXFile(string AName);

        ITDescriptor this[string AName] { get; }
        ITDescriptor Find(long AId);

        void Delete(string AName);
        void Rename(string AName, string ANewName);
        bool Exists(string AName);

        /// <summary>
        /// The number of tables & virtual files into the storage engine.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// The number of nodes that are kept in memory.
        /// </summary>
        int CacheSize { get; set; }

        /// <summary>
        /// Heap assigned to the StorageEngine instance.
        /// </summary>
        IHeap Heap { get; }

        void Commit();
        void Close();
    }
}
