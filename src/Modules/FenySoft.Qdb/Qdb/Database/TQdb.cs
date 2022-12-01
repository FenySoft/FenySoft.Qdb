using FenySoft.Core.IO;
using FenySoft.Qdb.Storage;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
  public static class TQdb
  {
    public static ITStorageEngine FromHeap(ITHeap heap)
    {
      return new TStorageEngine(heap);
    }

    public static ITStorageEngine FromStream(Stream stream)
    {
      ITHeap heap = new THeap(stream, false, AllocationStrategy.FromTheCurrentBlock);
      return FromHeap(heap);
    }

    public static ITStorageEngine FromMemory()
    {
      var stream = new MemoryStream();
      return FromStream(stream);
    }

    public static ITStorageEngine FromFile(string fileName)
    {
      var stream = new TOptimizedFileStream(fileName, FileMode.OpenOrCreate);
      return FromStream(stream);
    }
  }
}