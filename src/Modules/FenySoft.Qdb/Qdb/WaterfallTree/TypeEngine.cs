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
        public ITPersist<ITData> Persist { get; set; }
        public ITIndexerPersist<ITData> IndexerPersist { get; set; }

        public TypeEngine()
        {
        }

        private static TypeEngine Create(Type type)
        {
            TypeEngine descriptor = new TypeEngine();

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

        public static TypeEngine Default(Type type)
        {
            return map.GetOrAdd(type, Create(type));
        }
    }
}
