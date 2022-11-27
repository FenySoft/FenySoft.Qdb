using FenySoft.Core.Communication;
using FenySoft.Core.IO;
using FenySoft.Qdb.Remote;
using FenySoft.Qdb.Storage;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public static class STSdb
    {
        public static ITStorageEngine FromHeap(IHeap heap)
        {
            return new StorageEngine(heap);
        }

        public static ITStorageEngine FromStream(Stream stream)
        {
            IHeap heap = new Heap(stream, false, AllocationStrategy.FromTheCurrentBlock);

            return FromHeap(heap);
        }

        public static ITStorageEngine FromMemory()
        {
            var stream = new MemoryStream();

            return FromStream(stream);
        }

        public static ITStorageEngine FromFile(string fileName)
        {
            var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);

            return STSdb.FromStream(stream);
        }

        public static ITStorageEngine FromNetwork(string host, int port = 7182)
        {
            return new StorageEngineClient(host, port);
        }

        public static StorageEngineServer CreateServer(ITStorageEngine engine, int port = 7182)
        {
            TcpServer server = new TcpServer(port);
            StorageEngineServer engineServer = new StorageEngineServer(engine, server);

            return engineServer;
        }
    }
}