namespace FenySoft.Qdb.Storage
{
  public class TPointer
  {
    #region Fields..

    private readonly long FVersion;
    private TPtr FPtr;

    #endregion

    #region Properties..

    public long Version => FVersion;
    public TPtr Ptr => FPtr;
    public int RefCount { get; set; }
    public bool IsReserved { get; set; }

    #endregion
    
    #region Constructors..

    public TPointer(long AVersion, TPtr APtr)
    {
      FVersion = AVersion;
      FPtr = APtr;
      RefCount = 0;
    }

    #endregion

    #region Methods..

    public void Serialize(BinaryWriter AWriter)
    {
      AWriter.Write(FVersion);
      FPtr.Serialize(AWriter);
    }

    public static TPointer Deserialize(BinaryReader AReader)
    {
      long version = AReader.ReadInt64();
      TPtr ptr = TPtr.Deserialize(AReader);
      return new TPointer(version, ptr);
    }

    public override string ToString()
    {
      return $"Version {FVersion}, TPtr {FPtr}";
    }

    #endregion
  }
}