using System.Collections;

using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class XTablePortable<TKey, TRecord> : ITTable<TKey, TRecord>
    {
        public ITTable<ITData, ITData> Table { get; private set; }
        public ITTransformer<TKey, ITData> KeyTransformer { get; private set; }
        public ITTransformer<TRecord, ITData> RecordTransformer { get; private set; }

        public XTablePortable(ITTable<ITData, ITData> table, ITTransformer<TKey, ITData> keyTransformer = null, ITTransformer<TRecord, ITData> recordTransformer = null)
        {
            if (table == null)
                throw new ArgumentNullException("table");

            Table = table;

            if (keyTransformer == null)
                keyTransformer = new TDataTransformer<TKey>(table.Descriptor.KeyType);

            if (recordTransformer == null)
                recordTransformer = new TDataTransformer<TRecord>(table.Descriptor.RecordType);

            KeyTransformer = keyTransformer;
            RecordTransformer = recordTransformer;
        }

        #region ITTable<TKey, TRecord> Membres

        public TRecord this[TKey key]
        {
            get
            {
                ITData ikey = KeyTransformer.To(key);
                ITData irec = Table[ikey];
                
                return RecordTransformer.From(irec);
            }
            set
            {
                ITData ikey = KeyTransformer.To(key);
                ITData irec = RecordTransformer.To(value);

                Table[ikey] = irec;
            }
        }

        public void Replace(TKey key, TRecord record)
        {
            ITData ikey = KeyTransformer.To(key);
            ITData irec = RecordTransformer.To(record);

            Table.Replace(ikey, irec);
        }

        public void InsertOrIgnore(TKey key, TRecord record)
        {
            ITData ikey = KeyTransformer.To(key);
            ITData irec = RecordTransformer.To(record);

            Table.InsertOrIgnore(ikey, irec);
        }

        public void Delete(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            Table.Delete(ikey);
        }

        public void Delete(TKey fromKey, TKey toKey)
        {
            ITData ifrom = KeyTransformer.To(fromKey);
            ITData ito = KeyTransformer.To(toKey);

            Table.Delete(ifrom, ito);
        }

        public void Clear()
        {
            Table.Clear();
        }

        public bool Exists(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            return Table.Exists(ikey);
        }

        public bool TryGet(TKey key, out TRecord record)
        {
            ITData ikey = KeyTransformer.To(key);

            ITData irec;
            if (!Table.TryGet(ikey, out irec))
            {
                record = default(TRecord);
                return false;
            }

            record = RecordTransformer.From(irec);

            return true;
        }

        public TRecord Find(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            ITData irec = Table.Find(ikey);
            if (irec == null)
                return default(TRecord);

            TRecord record = RecordTransformer.From(irec);

            return record;
        }

        public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord)
        {
            ITData ikey = KeyTransformer.To(key);
            ITData idefaultRec = RecordTransformer.To(defaultRecord);
            ITData irec = Table.TryGetOrDefault(ikey, idefaultRec);

            TRecord record = RecordTransformer.From(irec);

            return record;
        }

        public KeyValuePair<TKey, TRecord>? FindNext(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindNext(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = KeyTransformer.From(kv.Value.Key);
            TRecord r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindAfter(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = KeyTransformer.From(kv.Value.Key);
            TRecord r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindPrev(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = KeyTransformer.From(kv.Value.Key);
            TRecord r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindBefore(TKey key)
        {
            ITData ikey = KeyTransformer.To(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindBefore(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = KeyTransformer.From(kv.Value.Key);
            TRecord r = RecordTransformer.From(kv.Value.Value);

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
        {
            foreach (var kv in Table.Forward())
            {
                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
        {
            ITData ifrom = hasFrom ? KeyTransformer.To(from) : null;
            ITData ito = hasTo ? KeyTransformer.To(to) : null;

            foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo))
            {
                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
        {
            foreach (var kv in Table.Backward())
            {
                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
        {
            ITData ito = hasTo ? KeyTransformer.To(to) : null;
            ITData ifrom = hasFrom ? KeyTransformer.To(from) : null;
            
            foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom))
            {
                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> FirstRow
        {
            get
            {
                KeyValuePair<ITData, ITData> kv = Table.FirstRow;

                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> LastRow
        {
            get
            {
                KeyValuePair<ITData, ITData> kv = Table.LastRow;

                TKey key = KeyTransformer.From(kv.Key);
                TRecord rec = RecordTransformer.From(kv.Value);

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public long Count()
        {
            return Table.Count();
        }

        public ITDescriptor Descriptor
        {
            get { return Table.Descriptor; }
        }

        #endregion

        #region IEnumerable<KeyValuePair<TKey, TRecord>> Members

        public IEnumerator<KeyValuePair<TKey, TRecord>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
