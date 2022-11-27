using FenySoft.Core.Data;
using FenySoft.Core.Persist;

using System.Collections.Concurrent;

namespace FenySoft.Qdb.WaterfallTree
{
    public class TypeEngine
    {
        private static readonly ConcurrentDictionary<Type, TypeEngine> map = new ConcurrentDictionary<Type, TypeEngine>();

        public IComparer<ITData> Comparer { get; set; }
        public IEqualityComparer<ITData> EqualityComparer { get; set; }
        public IPersist<ITData> Persist { get; set; }
        public IIndexerPersist<ITData> IndexerPersist { get; set; }

        public TypeEngine()
        {
        }

        private static TypeEngine Create(Type type)
        {
            TypeEngine descriptor = new TypeEngine();

            descriptor.Persist = new DataPersist(type, null, AllowNull.OnlyMembers);

            if (DataTypeUtils.IsAllPrimitive(type) || type == typeof(Guid))
            {
                descriptor.Comparer = new DataComparer(type);
                descriptor.EqualityComparer = new DataEqualityComparer(type);

                if (type != typeof(Guid))
                    descriptor.IndexerPersist = new DataIndexerPersist(type);
            }

            return descriptor;
        }

        public static TypeEngine Default(Type type)
        {
            return map.GetOrAdd(type, Create(type));
        }
    }
}
