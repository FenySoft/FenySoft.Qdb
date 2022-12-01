namespace FenySoft.Qdb.Storage
{
  public class TAtomicHeader
  {
    #region Constants..

    /// <summary>
    /// http://en.wikipedia.org/wiki/Advanced_Format
    /// http://www.idema.org
    /// </summary>
    public const int Size = 4 * 1024;
    public const int MaxTagData = 256;
    private const string Title = "Qdb 4.0";

    #endregion

    #region Fields..

    private byte[]? FTag;
    private int FVersion;
    private bool FUseCompression;

    #endregion
    
    #region Properties..
    
    /// <summary>
    /// System data location.
    /// </summary>
    public TPtr SystemData { get; set; }
    public bool UseCompression => FUseCompression;
    public byte[]? Tag
    {
      get { return FTag; }
      set
      {
        if (value != null && value.Length > MaxTagData)
          throw new ArgumentException("Tag");

        FTag = value;
      }
    }

    #endregion

    #region Constructors..

    public TAtomicHeader()
    {
    }
    
    public TAtomicHeader(bool AUseCompression)
    {
      FUseCompression= AUseCompression;
    }
    
    #endregion
    
    #region Methods..

    public void Serialize(Stream AStream)
    {
      byte[] buffer = new byte[Size];

      using (MemoryStream ms = new MemoryStream(buffer))
      {
        BinaryWriter writer = new BinaryWriter(ms);
        writer.Write(Title);
        writer.Write(FVersion);
        writer.Write(FUseCompression);
        //last flush location
        SystemData.Serialize(writer);

        //tag
        if (Tag == null)
          writer.Write((int)-1);
        else
        {
          writer.Write(Tag.Length);
          writer.Write(Tag);
        }
      }

      AStream.Seek(0, SeekOrigin.Begin);
      AStream.Write(buffer, 0, buffer.Length);
    }

    public static TAtomicHeader Deserialize(Stream AStream)
    {
      TAtomicHeader header = new TAtomicHeader();
      AStream.Seek(0, SeekOrigin.Begin);
      byte[] buffer = new byte[Size];

      if (AStream.Read(buffer, 0, Size) != Size)
        throw new Exception(String.Format("Invalid {0} header.", Title));

      using (MemoryStream ms = new MemoryStream(buffer))
      {
        BinaryReader reader = new BinaryReader(ms);
        string title = reader.ReadString();

        if (title != Title)
          throw new Exception(String.Format("Invalid {0} header.", Title));

        header.FVersion = reader.ReadInt32();
        header.FUseCompression = reader.ReadBoolean();
        //last flush location
        header.SystemData = TPtr.Deserialize(reader);
        //tag
        int tagLength = reader.ReadInt32();
        header.Tag = tagLength >= 0 ? reader.ReadBytes(tagLength) : null;
      }

      return header;
    }

    #endregion
  }
}