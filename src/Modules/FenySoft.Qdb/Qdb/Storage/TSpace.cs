namespace FenySoft.Qdb.Storage
{
  /// <summary>
  /// Strategies for free space allocation.
  /// </summary>
  public enum AllocationStrategy : byte
  {
    /// <summary>
    /// Searches for free space from the current block forwards (default behaviour).
    /// </summary>
    FromTheCurrentBlock,

    /// <summary>
    /// Always searches for free space from the beginning (reduces the space, but may affect the read/write speed).
    /// </summary>
    FromTheBeginning
  }

  public class TSpace
  {
    #region Fields..

    private int FActiveChunkIndex = -1;
    private readonly List<TPtr> FFree = new List<TPtr>(); //free chunks are always: ordered by position, not overlapped & not contiguous

    #endregion

    #region Properties..

    public AllocationStrategy Strategy { get; set; }
    public long FreeBytes { get; private set; }

    #endregion

    #region Constructors..

    public TSpace()
    {
      Strategy = AllocationStrategy.FromTheCurrentBlock;
    }

    #endregion

    #region Methods..

    public void Add(TPtr AFreeChunk)
    {
      if (FFree.Count == 0)
        FFree.Add(AFreeChunk);
      else
      {
        var last = FFree[FFree.Count - 1];

        if (AFreeChunk.Position > last.PositionPlusSize)
          FFree.Add(AFreeChunk);
        else if (AFreeChunk.Position == last.PositionPlusSize)
        {
          last.Size += AFreeChunk.Size;
          FFree[FFree.Count - 1] = last;
        }
        else
          throw new ArgumentException("Invalid ptr order.");
      }

      FreeBytes += AFreeChunk.Size;
    }

    public TPtr Alloc(long ASize)
    {
      if (FActiveChunkIndex < 0 || FActiveChunkIndex == FFree.Count - 1 || FFree[FActiveChunkIndex].Size < ASize)
      {
        int idx = 0;

        switch (Strategy)
        {
          case AllocationStrategy.FromTheCurrentBlock:
            idx = FActiveChunkIndex >= 0 && FActiveChunkIndex + 1 < FFree.Count - 1 ? FActiveChunkIndex + 1 : 0;
            break;
          case AllocationStrategy.FromTheBeginning:
            idx = 0;
            break;
          default:
            throw new NotSupportedException(Strategy.ToString());
        }

        for (int i = idx; i < FFree.Count; i++)
        {
          if (FFree[i].Size >= ASize)
          {
            FActiveChunkIndex = i;
            break;
          }
        }
      }

      TPtr ptr = FFree[FActiveChunkIndex];

      if (ptr.Size < ASize)
        throw new Exception("Not enough space.");

      long pos = ptr.Position;
      ptr.Position += ASize;
      ptr.Size -= ASize;

      if (ptr.Size > 0)
        FFree[FActiveChunkIndex] = ptr;
      else //if (ptr.Size == 0)
      {
        FFree.RemoveAt(FActiveChunkIndex);
        FActiveChunkIndex = -1; //search for active chunk at next alloc
      }

      FreeBytes -= ASize;
      return new TPtr(pos, ASize);
    }

    public void Free(TPtr APtr)
    {
      int idx = FFree.BinarySearch(APtr);

      if (idx >= 0)
        throw new ArgumentException("TSpace already freed.");

      idx = ~idx;

      if ((idx < FFree.Count && APtr.PositionPlusSize > FFree[idx]
              .Position) || (idx > 0 && APtr.Position < FFree[idx - 1]
              .PositionPlusSize))
        throw new ArgumentException("Can't free overlapped space.");

      bool merged = false;

      if (idx < FFree.Count) //try merge with right chunk
      {
        var p = FFree[idx];

        if (APtr.PositionPlusSize == p.Position)
        {
          p.Position -= APtr.Size;
          p.Size += APtr.Size;
          FFree[idx] = p;
          merged = true;
        }
      }

      if (idx > 0) //try merge with left chunk
      {
        var p = FFree[idx - 1];

        if (APtr.Position == p.PositionPlusSize)
        {
          if (merged)
          {
            p.Size += FFree[idx].Size;
            FFree[idx - 1] = p;
            FFree.RemoveAt(idx);

            if (FActiveChunkIndex >= idx)
              FActiveChunkIndex--;
          }
          else
          {
            p.Size += APtr.Size;
            FFree[idx - 1] = p;
            merged = true;
          }
        }
      }

      if (!merged)
        FFree.Insert(idx, APtr);

      FreeBytes += APtr.Size;
    }

    public void Serialize(BinaryWriter AWriter)
    {
      AWriter.Write((byte)Strategy);
      AWriter.Write(FActiveChunkIndex);
      AWriter.Write(FFree.Count);

      for (int i = 0; i < FFree.Count; i++)
        FFree[i].Serialize(AWriter);
    }

    public void Deserealize(BinaryReader AReader)
    {
      Strategy = (AllocationStrategy)AReader.ReadByte();
      FActiveChunkIndex = AReader.ReadInt32();
      int count = AReader.ReadInt32();
      FFree.Clear();
      FreeBytes = 0;

      for (int i = 0; i < count; i++)
      {
        var ptr = TPtr.Deserialize(AReader);
        FFree.Add(ptr);
        FreeBytes += ptr.Size;
      }
    }

    #endregion
  }
}