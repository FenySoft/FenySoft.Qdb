using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public interface ITOperationCollection : IEnumerable<ITOperation>
    {
        void Add(ITOperation operation);
        void AddRange(ITOperationCollection operations);
        void Clear();

        ITOperation this[int index] { get; }
        int Count { get; }
        int Capacity { get; }

        ITOperationCollection Midlle(int index, int count);
        int BinarySearch(ITData key, int index, int count);

        int CommonAction { get; }
        bool AreAllMonotoneAndPoint { get; }
        
        TLocator Locator { get; }
    }
}
