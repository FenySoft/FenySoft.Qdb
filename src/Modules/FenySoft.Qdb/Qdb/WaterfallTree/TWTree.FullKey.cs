using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public partial class TWTree
    {
        public struct TFullKey : IComparable<TFullKey>, IEquatable<TFullKey>
        {
            public readonly TLocator Locator;
            public readonly ITData Key;

            public TFullKey(TLocator locator, ITData key)
            {
                Locator = locator;
                Key = key;
            }

            public override string ToString()
            {
                return String.Format("TLocator = {0}, Key = {1}", Locator, Key);
            }

            #region IComparable<TLocator> Members

            public int CompareTo(TFullKey other)
            {
                int cmp = Locator.CompareTo(other.Locator);
                if (cmp != 0)
                    return cmp;

                return Locator.KeyComparer.Compare(Key, other.Key);
            }

            #endregion

            #region IEquatable<TLocator> Members

            public override int GetHashCode()
            {
                return Locator.GetHashCode() ^ Key.GetHashCode();
            }

            public bool Equals(TFullKey other)
            {
                if (!Locator.Equals(other.Locator))
                    return false;

                return Locator.KeyEqualityComparer.Equals(Key, other.Key);
            }

            #endregion
        }
    }
}
