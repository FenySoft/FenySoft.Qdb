using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FenySoft.Qdb.Storage
{
    [StructLayout(LayoutKind.Sequential)]
    public struct TPtr : IEquatable<TPtr>, IComparable<TPtr>
    {
        public static readonly TPtr NULL = new TPtr(0, 0);

        public long Position;
        public long Size;

        public const int SIZE = sizeof(long) + sizeof(long);

        [DebuggerStepThrough]
        public TPtr(long position, long size)
        {
            Position = position;
            Size = size;
        }

        #region IEquatable<TPtr> Members

        public bool Equals(TPtr other)
        {
            return Position == other.Position &&
                Size == other.Size;
        }

        #endregion

        #region IComparable<TPtr> Members

        public int CompareTo(TPtr other)
        {
            return Position.CompareTo(other.Position);
        }

        #endregion

        public override bool Equals(object obj)
        {
            if (obj is TPtr)
                return Equals((TPtr)obj);

            return false;
        }

        public override int GetHashCode()
        {
            return Position.GetHashCode() ^ Size.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format("({0}, {1})", Position, Size);
        }

        public static bool operator ==(TPtr ptr1, TPtr ptr2)
        {
            return ptr1.Equals(ptr2);
        }

        public static bool operator !=(TPtr ptr1, TPtr ptr2)
        {
            return !(ptr1 == ptr2);
        }

        public static TPtr operator +(TPtr ptr, long offset)
        {
            return new TPtr(ptr.Position + offset, ptr.Size);
        }

        /// <summary>
        /// Checking whether the pointer is invalid.
        /// </summary>
        public bool IsNull
        {
            get { return Equals(NULL); }
        }

        /// <summary>
        /// Returns index of the block after fragment.
        /// </summary>
        public long PositionPlusSize
        {
            get { return checked(Position + Size); }
        }

        #region Serialize/Deserialize

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Position);
            writer.Write(Size);
        }

        public static TPtr Deserialize(BinaryReader reader)
        {
            long position = reader.ReadInt64();
            long size = reader.ReadInt64();

            return new TPtr(position, size);
        }

        #endregion

        public bool Contains(long position)
        {
            return Position <= position && position < PositionPlusSize;
        }
    }
}
