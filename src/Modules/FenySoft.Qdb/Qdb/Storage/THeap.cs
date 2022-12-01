using FenySoft.Core.IO;
using FenySoft.Qdb.WaterfallTree;

using System.Diagnostics;
using System.IO.Compression;

namespace FenySoft.Qdb.Storage
{
  public class THeap : ITHeap
  {
    #region Fields..

    private readonly object FSyncRoot = new object();
    private TAtomicHeader FHeader;
    private readonly TSpace FSpace;

    //updated every time after Serialize() invocation.
    private long FMaxPositionPlusSize;

    //handle -> pointer
    private readonly Dictionary<long, TPointer> FUsed;
    private readonly Dictionary<long, TPointer> FReserved;

    private long FCurrentVersion;
    private long FMaxHandle;

    #endregion

    #region Properties..

    public Stream Stream { get; private set; }

    public AllocationStrategy Strategy
    {
      get
      {
        lock (FSyncRoot)
          return FSpace.Strategy;
      }
      set
      {
        lock (FSyncRoot)
          FSpace.Strategy = value;
      }
    }

    public byte[] Tag
    {
      get
      {
        lock (FSyncRoot)
          return FHeader.Tag;
      }
      set
      {
        lock (FSyncRoot)
          FHeader.Tag = value;
      }
    }

    public long DataSize
    {
      get
      {
        lock (FSyncRoot)
          return FUsed.Sum(AKv => AKv.Value.Ptr.Size);
      }
    }

    public long Size
    {
      get
      {
        lock (FSyncRoot)
          return Stream.Length;
      }
    }

    public bool UseCompression
    {
      get
      {
        lock (FSyncRoot)
          return FHeader.UseCompression;
      }
    }

    public long CurrentVersion
    {
      get
      {
        lock (FSyncRoot)
          return FCurrentVersion;
      }
    }

    #endregion

    #region Constructors..

    public THeap(Stream AStream, bool AUseCompression = false, AllocationStrategy AStrategy = AllocationStrategy.FromTheCurrentBlock)
    {
      AStream.Seek(0, SeekOrigin.Begin); //support Seek?
      Stream = AStream;
      FSpace = new TSpace();
      FUsed = new Dictionary<long, TPointer>();
      FReserved = new Dictionary<long, TPointer>();

      if (AStream.Length < TAtomicHeader.Size) //create new
      {
        FHeader = new TAtomicHeader(AUseCompression);
        FSpace.Add(new TPtr(TAtomicHeader.Size, long.MaxValue - TAtomicHeader.Size));
      }
      else //open exist (ignore the useCompression flag)
      {
        FHeader = TAtomicHeader.Deserialize(Stream);
        AStream.Seek(FHeader.SystemData.Position, SeekOrigin.Begin);
        Deserialize(new BinaryReader(AStream));
        //manual alloc header.SystemData
        var ptr = FSpace.Alloc(FHeader.SystemData.Size);

        if (ptr.Position != FHeader.SystemData.Position)
          throw new Exception("Logical error.");
      }

      Strategy = AStrategy;
      FCurrentVersion++;
    }

    public THeap(string AFileName, bool AUseCompression = false, AllocationStrategy AStrategy = AllocationStrategy.FromTheCurrentBlock) : this(new TOptimizedFileStream(AFileName, FileMode.OpenOrCreate), AUseCompression, AStrategy)
    {
    }

    #endregion

    #region Methods..

    private void FreeOldVersions()
    {
      List<long> forRemove = new List<long>();

      foreach (var kv in FReserved)
      {
        var handle = kv.Key;
        var pointer = kv.Value;

        if (pointer.RefCount > 0)
          continue;

        FSpace.Free(pointer.Ptr);
        forRemove.Add(handle);
      }

      foreach (var handle in forRemove)
        FReserved.Remove(handle);
    }

    private void InternalWrite(long APosition, int AOriginalCount, byte[] ABuffer, int AIndex, int ACount)
    {
      BinaryWriter writer = new BinaryWriter(Stream);
      Stream.Seek(APosition, SeekOrigin.Begin);

      if (UseCompression)
        writer.Write(AOriginalCount);

      writer.Write(ABuffer, AIndex, ACount);
    }

    private byte[] InternalRead(long APosition, long ASize)
    {
      BinaryReader reader = new BinaryReader(Stream);
      Stream.Seek(APosition, SeekOrigin.Begin);

      byte[] buffer;

      if (!UseCompression)
        buffer = reader.ReadBytes((int)ASize);
      else
      {
        byte[] raw = new byte[reader.ReadInt32()];
        buffer = reader.ReadBytes((int)ASize - sizeof(int));

        using (MemoryStream stream = new MemoryStream(buffer))
        {
          using (DeflateStream decompress = new DeflateStream(stream, CompressionMode.Decompress))
            decompress.Read(raw, 0, raw.Length);
        }

        buffer = raw;
      }

      return buffer;
    }

    private void Serialize(BinaryWriter AWriter)
    {
      FMaxPositionPlusSize = TAtomicHeader.Size;
      AWriter.Write(FMaxHandle);
      AWriter.Write(FCurrentVersion);
      //write free
      FSpace.Serialize(AWriter);
      //write used
      AWriter.Write(FUsed.Count);

      foreach (var kv in FUsed)
      {
        AWriter.Write(kv.Key);
        kv.Value.Serialize(AWriter);
        long posPlusSize = kv.Value.Ptr.PositionPlusSize;

        if (posPlusSize > FMaxPositionPlusSize)
          FMaxPositionPlusSize = posPlusSize;
      }

      //write reserved
      AWriter.Write(FReserved.Count);

      foreach (var kv in FReserved)
      {
        AWriter.Write(kv.Key);
        kv.Value.Serialize(AWriter);
        long posPlusSize = kv.Value.Ptr.PositionPlusSize;

        if (posPlusSize > FMaxPositionPlusSize)
          FMaxPositionPlusSize = posPlusSize;
      }
    }

    private void Deserialize(BinaryReader AReader)
    {
      FMaxHandle = AReader.ReadInt64();
      FCurrentVersion = AReader.ReadInt64();
      //read free
      FSpace.Deserealize(AReader);
      //read used
      int count = AReader.ReadInt32();

      for (int i = 0; i < count; i++)
      {
        var handle = AReader.ReadInt64();
        var pointer = TPointer.Deserialize(AReader);
        FUsed.Add(handle, pointer);
      }

      //read reserved
      count = AReader.ReadInt32();

      for (int i = 0; i < count; i++)
      {
        var handle = AReader.ReadInt64();
        var pointer = TPointer.Deserialize(AReader);
        FReserved.Add(handle, pointer);
      }
    }

    public long ObtainNewHandle()
    {
      lock (FSyncRoot)
        return FMaxHandle++;
    }

    public void Release(long AHandle)
    {
      lock (FSyncRoot)
      {
        TPointer pointer;

        if (!FUsed.TryGetValue(AHandle, out pointer))
          return; //throw new ArgumentException("handle");

        if (pointer.Version == FCurrentVersion)
          FSpace.Free(pointer.Ptr);
        else
        {
          pointer.IsReserved = true;
          FReserved.Add(AHandle, pointer);
        }

        FUsed.Remove(AHandle);
      }
    }

    public bool Exists(long AHandle)
    {
      lock (FSyncRoot)
        return FUsed.ContainsKey(AHandle);
    }

    /// <summary>
    /// Before writting, handle must be obtained (registered).
    /// New block will be written always with version = CurrentVersion
    /// If new block is written to handle and the last block of this handle have same version with the new one, occupied space by the last block will be freed.
    /// </summary>
    public void Write(long AHandle, byte[] ABuffer, int AIndex, int ACount)
    {
      int originalCount = ACount;

      if (UseCompression)
      {
        using (MemoryStream stream = new MemoryStream())
        {
          using (DeflateStream compress = new DeflateStream(stream, CompressionMode.Compress, true))
            compress.Write(ABuffer, AIndex, ACount);

          ABuffer = stream.GetBuffer();
          AIndex = 0;
          ACount = (int)stream.Length;
        }
      }

      lock (FSyncRoot)
      {
        if (AHandle >= FMaxHandle)
          throw new ArgumentException("Invalid handle.");

        TPointer pointer;

        if (FUsed.TryGetValue(AHandle, out pointer))
        {
          if (pointer.Version == FCurrentVersion)
            FSpace.Free(pointer.Ptr);
          else
          {
            pointer.IsReserved = true;
            FReserved.Add(AHandle, pointer);
          }
        }

        long size = UseCompression ? sizeof(int) + ACount : ACount;
        TPtr ptr = FSpace.Alloc(size);
        FUsed[AHandle] = pointer = new TPointer(FCurrentVersion, ptr);
        InternalWrite(ptr.Position, originalCount, ABuffer, AIndex, ACount);
      }
    }

    public byte[] Read(long AHandle)
    {
      lock (FSyncRoot)
      {
        TPointer pointer;

        if (!FUsed.TryGetValue(AHandle, out pointer))
          throw new ArgumentException("No such handle or data exists.");

        TPtr ptr = pointer.Ptr;
        Debug.Assert(ptr != TPtr.Null);
        return InternalRead(ptr.Position, ptr.Size);
      }
    }

    public void Commit()
    {
      lock (FSyncRoot)
      {
        Stream.Flush();

        FreeOldVersions();

        using (MemoryStream ms = new MemoryStream())
        {
          if (FHeader.SystemData != TPtr.Null)
            FSpace.Free(FHeader.SystemData);

          Serialize(new BinaryWriter(ms));
          TPtr ptr = FSpace.Alloc(ms.Length);
          Stream.Seek(ptr.Position, SeekOrigin.Begin);
          Stream.Write(ms.GetBuffer(), 0, (int)ms.Length);
          FHeader.SystemData = ptr;
          //atomic write
          FHeader.Serialize(Stream);

          if (ptr.PositionPlusSize > FMaxPositionPlusSize)
            FMaxPositionPlusSize = ptr.PositionPlusSize;
        }

        Stream.Flush();

        //try to truncate the stream
        if (Stream.Length > FMaxPositionPlusSize)
          Stream.SetLength(FMaxPositionPlusSize);

        FCurrentVersion++;
      }
    }

    public void Close()
    {
      lock (FSyncRoot)
        Stream.Close();
    }

    public IEnumerable<KeyValuePair<long, byte[]>> GetLatest(long AAtVersion)
    {
      List<KeyValuePair<long, TPointer>> list = new List<KeyValuePair<long, TPointer>>();

      lock (FSyncRoot)
      {
        foreach (var kv in FUsed.Union(FReserved))
        {
          var handle = kv.Key;
          var pointer = kv.Value;

          if (pointer.Version >= AAtVersion && pointer.Version < FCurrentVersion)
          {
            list.Add(new KeyValuePair<long, TPointer>(handle, pointer));
            pointer.RefCount++;
          }
        }
      }

      foreach (var kv in list)
      {
        var handle = kv.Key;
        var pointer = kv.Value;
        byte[] buffer;

        lock (FSyncRoot)
        {
          buffer = InternalRead(pointer.Ptr.Position, pointer.Ptr.Size);
          pointer.RefCount--;

          if (pointer.IsReserved && pointer.RefCount <= 0)
          {
            FSpace.Free(pointer.Ptr);
            FReserved.Remove(handle);
          }
        }

        yield return new KeyValuePair<long, byte[]>(handle, buffer);
      }
    }

    public KeyValuePair<long, TPtr>[] GetUsedSpace()
    {
      lock (FSyncRoot)
      {
        KeyValuePair<long, TPtr>[] array = new KeyValuePair<long, TPtr>[FUsed.Count + FReserved.Count];
        int idx = 0;

        foreach (var kv in FUsed.Union(FReserved))
          array[idx++] = new KeyValuePair<long, TPtr>(kv.Value.Version, kv.Value.Ptr);

        return array;
      }
    }

    #endregion
  }
}