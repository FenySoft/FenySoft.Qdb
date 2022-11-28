using FenySoft.Core.Data;
using FenySoft.Core.Collections;

namespace FenySoft.Qdb.WaterfallTree
{
    public interface IApply
    {
        /// <summary>
        /// Compact the operations and returns true, if the collection was modified.
        /// </summary>
        bool Internal(IOperationCollection operations);

        bool Leaf(IOperationCollection operations, ITOrderedSet<ITData, ITData> data);

        Locator Locator { get; }
    }
}
