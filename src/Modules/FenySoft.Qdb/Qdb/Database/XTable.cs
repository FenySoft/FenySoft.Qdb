using System.Collections;

using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class XTable<TKey, TRecord> : ITTable<TKey, TRecord>
    {
        public ITTable<ITData, ITData> Table { get; private set; }

        public XTable(ITTable<ITData, ITData> table)
        {
            if (table == null)
                throw new ArgumentNullException("table");
            
            Table = table;
        }

        #region ITTable<TKey, TRecord> Membres

        public TRecord this[TKey key]
        {
            get
            {
                ITData ikey = new Data<TKey>(key);
                ITData irec = Table[ikey];

                return ((Data<TRecord>)irec).Value;
            }
            set
            {
                ITData ikey = new Data<TKey>(key);
                ITData irec = new Data<TRecord>(value);

                Table[ikey] = irec;
            }
        }

        public void Replace(TKey key, TRecord record)
        {
            ITData ikey = new Data<TKey>(key);
            ITData irec = new Data<TRecord>(record);

            Table.Replace(ikey, irec);
        }

        public void InsertOrIgnore(TKey key, TRecord record)
        {
            ITData ikey = new Data<TKey>(key);
            ITData irec = new Data<TRecord>(record);

            Table.InsertOrIgnore(ikey, irec);
        }

        public void Delete(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            Table.Delete(ikey);
        }

        public void Delete(TKey fromKey, TKey toKey)
        {
            ITData ifrom = new Data<TKey>(fromKey);
            ITData ito = new Data<TKey>(toKey);

            Table.Delete(ifrom, ito);
        }

        public void Clear()
        {
            Table.Clear();
        }

        public bool Exists(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            return Table.Exists(ikey);
        }

        public bool TryGet(TKey key, out TRecord record)
        {
            ITData ikey = new Data<TKey>(key);

            ITData irec;
            if (!Table.TryGet(ikey, out irec))
            {
                record = default(TRecord);
                return false;
            }

            record = ((Data<TRecord>)irec).Value;

            return true;
        }

        public TRecord Find(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            ITData irec = Table.Find(ikey);
            if (irec == null)
                return default(TRecord);

            TRecord record = ((Data<TRecord>)irec).Value;

            return record;
        }

        public TRecord TryGetOrDefault(TKey key, TRecord defaultRecord)
        {
            ITData ikey = new Data<TKey>(key);
            ITData idefaultRec = new Data<TRecord>(defaultRecord);
            ITData irec = Table.TryGetOrDefault(ikey, idefaultRec);

            TRecord record = ((Data<TRecord>)irec).Value;

            return record;
        }

        public KeyValuePair<TKey, TRecord>? FindNext(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindNext(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = ((Data<TKey>)kv.Value.Key).Value;
            TRecord r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindAfter(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindAfter(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = ((Data<TKey>)kv.Value.Key).Value;
            TRecord r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindPrev(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindPrev(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = ((Data<TKey>)kv.Value.Key).Value;
            TRecord r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public KeyValuePair<TKey, TRecord>? FindBefore(TKey key)
        {
            ITData ikey = new Data<TKey>(key);

            KeyValuePair<ITData, ITData>? kv = Table.FindBefore(ikey);
            if (!kv.HasValue)
                return null;

            TKey k = ((Data<TKey>)kv.Value.Key).Value;
            TRecord r = ((Data<TRecord>)kv.Value.Value).Value;

            return new KeyValuePair<TKey, TRecord>(k, r);
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward()
        {
            foreach (var kv in Table.Forward())
            {
                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Forward(TKey from, bool hasFrom, TKey to, bool hasTo)
        {
            ITData ifrom = hasFrom ? new Data<TKey>(from) : null;
            ITData ito = hasTo ? new Data<TKey>(to) : null;

            foreach (var kv in Table.Forward(ifrom, hasFrom, ito, hasTo))
            {
                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward()
        {
            foreach (var kv in Table.Backward())
            {
                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public IEnumerable<KeyValuePair<TKey, TRecord>> Backward(TKey to, bool hasTo, TKey from, bool hasFrom)
        {
            ITData ito = hasTo ? new Data<TKey>(to) : null;
            ITData ifrom = hasFrom ? new Data<TKey>(from) : null;

            foreach (var kv in Table.Backward(ito, hasTo, ifrom, hasFrom))
            {
                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

                yield return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> FirstRow
        {
            get
            {
                KeyValuePair<ITData, ITData> kv = Table.FirstRow;

                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

                return new KeyValuePair<TKey, TRecord>(key, rec);
            }
        }

        public KeyValuePair<TKey, TRecord> LastRow
        {
            get
            {
                KeyValuePair<ITData, ITData> kv = Table.LastRow;

                TKey key = ((Data<TKey>)kv.Key).Value;
                TRecord rec = ((Data<TRecord>)kv.Value).Value;

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
