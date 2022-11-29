using FenySoft.Core.Collections;
using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class TOrderedSetFactory : ITOrderedSetFactory
    {
        public TLocator Locator { get; private set; }
        
        public TOrderedSetFactory(TLocator locator)
        {
            Locator = locator;
        }

        public ITOrderedSet<ITData, ITData> Create()
        {
            var data = new TOrderedSet<ITData, ITData>(Locator.KeyComparer, Locator.KeyEqualityComparer);
            
            return data;
        }
    }
}
