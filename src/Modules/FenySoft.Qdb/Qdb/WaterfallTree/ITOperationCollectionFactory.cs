namespace FenySoft.Qdb.WaterfallTree
{
    public interface ITOperationCollectionFactory
    {
        ITOperationCollection Create(int capacity);
        ITOperationCollection Create(ITOperation[] operations, int commonAction, bool areAllMonotoneAndPoint);
    }
}
