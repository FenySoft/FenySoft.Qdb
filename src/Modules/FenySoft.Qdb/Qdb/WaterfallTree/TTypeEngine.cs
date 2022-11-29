using FenySoft.Core.Data;
using FenySoft.Core.Persist;

using System.Collections.Concurrent;

namespace FenySoft.Qdb.WaterfallTree
{
    public class TTypeEngine
    {
        private static readonly ConcurrentDictionary<Type, TTypeEngine> map = new ConcurrentDictionary<Type, TTypeEngine>();

        public IComparer<ITData> Comparer { get; set; }
        public IEqualityComparer<ITData> EqualityComparer { get; set; }
        public ITPersist<ITData> Persist { get; set; }
        public ITIndexerPersist<ITData> IndexerPersist { get; set; }

        public TTypeEngine()
        {
        }

        private static TTypeEngine Create(Type type)
        {
            TTypeEngine descriptor = new TTypeEngine();

            descriptor.Persist = new TDataPersist(type, null, AllowNull.OnlyMembers);

            if (TDataTypeUtils.IsAllPrimitive(type) || type == typeof(Guid))
            {
                descriptor.Comparer = new TDataComparer(type);
                descriptor.EqualityComparer = new TDataEqualityComparer(type);

                if (type != typeof(Guid))
                    descriptor.IndexerPersist = new TDataIndexerPersist(type);
            }

            return descriptor;
        }

        public static TTypeEngine Default(Type type)
        {
            return map.GetOrAdd(type, Create(type));
        }
    }
}
