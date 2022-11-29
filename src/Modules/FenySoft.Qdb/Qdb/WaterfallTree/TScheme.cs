using FenySoft.Core.Data;

using System.Collections;
using System.Collections.Concurrent;

namespace FenySoft.Qdb.WaterfallTree
{
    public class TScheme : IEnumerable<KeyValuePair<long, TLocator>>
    {
        public const byte VERSION = 40;

        private long locatorID = TLocator.MIN.ID;

        private ConcurrentDictionary<long, TLocator> map = new ConcurrentDictionary<long, TLocator>();

        private long ObtainPathID()
        {
            return Interlocked.Increment(ref locatorID);
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(VERSION);

            writer.Write(locatorID);
            writer.Write(map.Count);

            foreach (var kv in map)
            {
                TLocator locator = (TLocator)kv.Value;
                locator.Serialize(writer);
            }
        }

        public static TScheme Deserialize(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid TScheme version.");

            TScheme scheme = new TScheme();

            scheme.locatorID = reader.ReadInt64();
            int count = reader.ReadInt32();

            for (int i = 0; i < count; i++)
            {
                var locator = TLocator.Deserialize(reader);

                scheme.map[locator.ID] = locator;

                //Do not prepare the locator yet
            }

            return scheme;
        }

        public TLocator this[long id]
        {
            get { return map[id]; }
        }

        public TLocator Create(string name, int structureType, TDataType keyDataType, TDataType recordDataType, Type keyType, Type recordType)
        {
            var id = ObtainPathID();

            var locator = new TLocator(id, name, structureType, keyDataType, recordDataType, keyType, recordType);

            map[id] = locator;

            return locator;
        }

        public int Count
        {
            get { return map.Count; }
        }

        public IEnumerator<KeyValuePair<long, TLocator>> GetEnumerator()
        {
            return map.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}