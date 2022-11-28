using System.Collections;
using System.Diagnostics;

using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class StorageEngine : WTree, ITStorageEngine
    {
        //user scheme
        private Dictionary<string, Item1> map = new Dictionary<string, Item1>();

        private readonly object SyncRoot = new object();

        public StorageEngine(IHeap heap)
            : base(heap)
        {
            foreach (var locator in GetAllLocators())
            {
                if (locator.IsDeleted)
                    continue;
                
                Item1 item = new Item1(locator, null);

                map[locator.Name] = item;
            }
        }

        private Item1 Obtain(string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType)
        {
            Debug.Assert(keyDataType != null);
            Debug.Assert(recordDataType != null);

            Item1 item;
            if (!map.TryGetValue(name, out item))
            {
                if (keyType == null)
                    keyType = TDataTypeUtils.BuildType(keyDataType);
                if (recordType == null)
                    recordType = TDataTypeUtils.BuildType(recordDataType);

                var locator = CreateLocator(name, structureType, keyDataType, recordDataType, keyType, recordType);
                XTablePortable table = new XTablePortable(this, locator);

                map[name] = item = new Item1(locator, table);
            }
            else
            {
                var locator = item.Locator;

                if (locator.StructureType != structureType)
                    throw new ArgumentException(String.Format("Invalid structure type for '{0}'", name));

                if (keyDataType != locator.KeyDataType)
                    throw new ArgumentException("AKeyDataType");

                if (recordDataType != locator.RecordDataType)
                    throw new ArgumentException("ARecordDataType");

                if (locator.KeyType == null)
                    locator.KeyType = TDataTypeUtils.BuildType(keyDataType);
                else
                {
                    if (keyType != null && keyType != locator.KeyType)
                        throw new ArgumentException(String.Format("Invalid AKeyDataType for table '{0}'", name));
                }

                if (locator.RecordType == null)
                    locator.RecordType = TDataTypeUtils.BuildType(recordDataType);
                else
                {
                    if (recordType != null && recordType != locator.RecordType)
                        throw new ArgumentException(String.Format("Invalid ARecordDataType for table '{0}'", name));
                }

                locator.AccessTime = DateTime.Now;
            }

            if (!item.Locator.IsReady)
                item.Locator.Prepare();

            if (item.Table == null)
                item.Table = new XTablePortable(this, item.Locator);

            return item;
        }

        #region ITStorageEngine

        public ITTable<ITData, ITData> OpenXTablePortable(string AName, TDataType AKeyDataType, TDataType ARecordDataType)
        {
            lock (SyncRoot)
            {
                var item = Obtain(AName, StructureType.XTABLE, AKeyDataType, ARecordDataType, null, null);

                return item.Table;
            }
        }

        public ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName, TDataType AKeyDataType, TDataType ARecordDataType, ITTransformer<TKey, ITData> AKeyTransformer, ITTransformer<TRecord, ITData> ARecordTransformer)
        {
            lock (SyncRoot)
            {
                var item = Obtain(AName, StructureType.XTABLE, AKeyDataType, ARecordDataType, null, null);

                if (item.Portable == null)
                    item.Portable = new XTablePortable<TKey, TRecord>(item.Table, AKeyTransformer, ARecordTransformer);

                return (ITTable<TKey, TRecord>)item.Portable;
            }
        }

        public ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName)
        {
            TDataType keyDataType = TDataTypeUtils.BuildDataType(typeof(TKey));
            TDataType recordDataType = TDataTypeUtils.BuildDataType(typeof(TRecord));

            return OpenXTablePortable<TKey, TRecord>(AName, keyDataType, recordDataType, null, null);
        }

        public ITTable<TKey, TRecord> OpenXTable<TKey, TRecord>(string AName)
        {
            lock (SyncRoot)
            {
                Type keyType = typeof(TKey);
                Type recordType = typeof(TRecord);

                TDataType keyDataType = TDataTypeUtils.BuildDataType(keyType);
                TDataType recordDataType = TDataTypeUtils.BuildDataType(recordType);

                var item = Obtain(AName, StructureType.XTABLE, keyDataType, recordDataType, keyType, recordType);

                if (item.Direct == null)
                    item.Direct = new XTable<TKey, TRecord>(item.Table);

                return (XTable<TKey, TRecord>)item.Direct;
            }
        }

        public XFile OpenXFile(string AName)
        {
            lock (SyncRoot)
            {
                var item = Obtain(AName, StructureType.XFILE, TDataType.Int64, TDataType.ByteArray, typeof(long), typeof(byte[]));

                if (item.File == null)
                    item.File = new XFile(item.Table);

                return item.File;
            }
        }

        public ITDescriptor this[string AName]
        {
            get
            {
                lock (SyncRoot)
                {
                    Item1 item;
                    if (!map.TryGetValue(AName, out item))
                        return null;

                    return item.Locator;
                }
            }
        }

        public ITDescriptor Find(long AId)
        {
            lock (SyncRoot)
                return GetLocator(AId);
        }

        public void Delete(string AName)
        {
            lock (SyncRoot)
            {
                Item1 item;
                if (!map.TryGetValue(AName, out item))
                    return;

                map.Remove(AName);

                if (item.Table != null)
                {
                    item.Table.Clear();
                    item.Table.Flush();
                }

                item.Locator.IsDeleted = true;
            }
        }

        public void Rename(string AName, string ANewName)
        {
            lock (SyncRoot)
            {
                if (map.ContainsKey(ANewName))
                    return;

                Item1 item;
                if (!map.TryGetValue(AName, out item))
                    return;

                item.Locator.Name = ANewName;

                map.Remove(AName);
                map.Add(ANewName, item);
            }
        }

        public bool Exists(string AName)
        {
            lock (SyncRoot)
                return map.ContainsKey(AName);
        }

        public int Count
        {
            get
            {
                lock (SyncRoot)
                    return map.Count;
            }
        }

        public override void Commit()
        {
            lock (SyncRoot)
            {
                foreach (var kv in map)
                {
                    var table = kv.Value.Table;

                    if (table != null)
                    {
                        if (table.IsModified)
                            table.Locator.ModifiedTime = DateTime.Now;

                        table.Flush();
                    }
                }

                base.Commit();

                foreach (var kv in map)
                {
                    var table = kv.Value.Table;

                    if (table != null)
                        table.IsModified = false;
                }
            }
        }

        public override void Close()
        {
            base.Close();
        }

        public IEnumerator<ITDescriptor> GetEnumerator()
        {
            lock (SyncRoot)
            {
                return map.Select(x => (ITDescriptor)x.Value.Locator).GetEnumerator();
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private class Item1
        {
            public Locator Locator;
            public XTablePortable Table;

            public ITable Direct;
            public ITable Portable;
            public XFile File;            

            public Item1(Locator locator, XTablePortable table)
            {
                Locator = locator;
                Table = table;
            }
        }
    }
}
