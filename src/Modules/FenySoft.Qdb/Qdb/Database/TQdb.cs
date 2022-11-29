using FenySoft.Core.IO;
using FenySoft.Qdb.Remote;
using FenySoft.Qdb.Storage;
using FenySoft.Qdb.WaterfallTree;
using FenySoft.Remote;

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

        public static ITStorageEngine FromNetwork(string host, int port = 7182)
        {
            return new TStorageEngineClient(host, port);
        }

        public static TStorageEngineServer CreateServer(ITStorageEngine engine, int port = 7182)
        {
            TTcpServer server = new TTcpServer(port);
            TStorageEngineServer engineServer = new TStorageEngineServer(engine, server);

            return engineServer;
        }
    }
}