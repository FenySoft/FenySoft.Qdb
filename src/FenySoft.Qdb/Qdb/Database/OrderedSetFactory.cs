using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class OrderedSetFactory : IOrderedSetFactory
    {
        public Locator Locator { get; private set; }
        
        public OrderedSetFactory(Locator locator)
        {
            Locator = locator;
        }

        public IOrderedSet<ITData, ITData> Create()
        {
            var data = new OrderedSet<ITData, ITData>(Locator.KeyComparer, Locator.KeyEqualityComparer);
            
            return data;
        }
    }
}
