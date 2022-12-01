using System.Diagnostics;
using System.Runtime.InteropServices;

namespace FenySoft.Qdb.Storage
{
  [StructLayout(LayoutKind.Sequential)]
  public struct TPtr : IEquatable<TPtr>, IComparable<TPtr>
  {
    #region Constants..

    public static readonly TPtr Null = new TPtr(0, 0);

    #endregion

    #region Fields..

    #endregion

    #region Properties..

    /// <summary>
    /// Returns index of the block after fragment.
    /// </summary>
    public long PositionPlusSize => checked(Position + Size);

    public long Position { get; set; }

    public long Size { get; set; }

    /// <summary>
    /// Checking whether the pointer is invalid.
    /// </summary>
    public bool IsNull => Equals(Null);

    #endregion

    #region Constructors..

    [DebuggerStepThrough]
    public TPtr(long APosition, long ASize)
    {
      Position = APosition;
      Size = ASize;
    }

    #endregion

    #region IEquatable<TPtr> Members

    public bool Equals(TPtr AOther)
    {
      return Position == AOther.Position && Size == AOther.Size;
    }

    #endregion

    #region IComparable<TPtr> Members

    public int CompareTo(TPtr AOther)
    {
      return Position.CompareTo(AOther.Position);
    }

    #endregion

    #region Object overrides..

    public override bool Equals(object? AObject)
    {
      if (AObject is TPtr)
        return Equals((TPtr)AObject);
      
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

    public bool Contains(long APosition)
    {
      return Position <= APosition && APosition < PositionPlusSize;
    }

    #endregion

    #region Operators..

    public static bool operator ==(TPtr APtr1, TPtr APtr2)
    {
      return APtr1.Equals(APtr2);
    }

    public static bool operator !=(TPtr APtr1, TPtr APtr2)
    {
      return !(APtr1 == APtr2);
    }

    public static TPtr operator +(TPtr APtr, long AOffset)
    {
      return new TPtr(APtr.Position + AOffset, APtr.Size);
    }

    #endregion
    
    #region Serialize/Deserialize

    public void Serialize(BinaryWriter AWriter)
    {
      AWriter.Write(Position);
      AWriter.Write(Size);
    }

    public static TPtr Deserialize(BinaryReader AReader)
    {
      long position = AReader.ReadInt64();
      long size = AReader.ReadInt64();

      return new TPtr(position, size);
    }

    #endregion
  }
}