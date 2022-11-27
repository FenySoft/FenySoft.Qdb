using FenySoft.Core.Data;
using FenySoft.Core.Collections;

namespace FenySoft.Qdb.WaterfallTree
{
    public interface IDataContainer : IOrderedSet<ITData, ITData>
    {
        double FillPercentage { get; }

        bool IsEmpty { get; }
        /// <summary>
        /// Exclude and returns the right half part of the ordered set.
        /// </summary>
        IDataContainer Split(double percentage);

        /// <summary>
        /// Merge the specified set to the current set. The engine ensures, that all keys from the one set are less/greater than all keys from the other set.
        /// </summary>
        void Merge(IDataContainer data);

        ITData FirstKey { get; }
        ITData LastKey { get; }
    }
}
