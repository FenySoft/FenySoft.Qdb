using FenySoft.Core.Data;
using FenySoft.Core.Collections;

namespace FenySoft.Qdb.WaterfallTree
{
    public interface ITApply
    {
        /// <summary>
        /// Compact the operations and returns true, if the collection was modified.
        /// </summary>
        bool Internal(ITOperationCollection operations);

        bool Leaf(ITOperationCollection operations, ITOrderedSet<ITData, ITData> data);

        TLocator Locator { get; }
    }
}
