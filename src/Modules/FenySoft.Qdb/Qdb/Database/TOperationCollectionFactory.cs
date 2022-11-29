using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class TOperationCollectionFactory : ITOperationCollectionFactory
    {
        public readonly TLocator Locator;
        
        public TOperationCollectionFactory(TLocator locator)
        {
            Locator = locator;
        }

        public ITOperationCollection Create(int capacity)
        {
            return new TOperationCollection(Locator, capacity);
        }

        public ITOperationCollection Create(ITOperation[] operations, int commonAction, bool areAllMonotoneAndPoint)
        {
            return new TOperationCollection(Locator, operations, commonAction, areAllMonotoneAndPoint);
        }
    }
}
