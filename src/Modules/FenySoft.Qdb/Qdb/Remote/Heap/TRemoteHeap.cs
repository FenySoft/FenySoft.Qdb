using FenySoft.Qdb.WaterfallTree;
using FenySoft.Remote;

namespace FenySoft.Qdb.Remote.Heap
{
    public class RemoteHeap : ITHeap
    {
        public TClientConnection Client { get; private set; }

        public RemoteHeap(string host, int port)
        {
            Client = new TClientConnection(host, port);
            Client.Start();
        }

        #region ITHeap members

        public long ObtainNewHandle()
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TObtainHandleCommand.WriteRequest(writer);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
            packet.Wait();

            return TObtainHandleCommand.ReadResponse(new BinaryReader(packet.Response)).Handle;
        }

        public void Release(long handle)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TReleaseHandleCommand.WriteRequest(writer, handle);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
        }

        public bool Exists(long handle)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            THandleExistCommand.WriteRequest(writer, handle);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
            packet.Wait();

            return THandleExistCommand.ReadResponse(new BinaryReader(packet.Response)).Exist;
        }

        public void Write(long handle, byte[] buffer, int index, int count)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TWriteCommand.WriteRequest(writer, handle, index, count, buffer);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
        }

        public byte[] Read(long handle)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TReadCommand.WriteRequest(writer, handle);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
            packet.Wait();

            return TReadCommand.ReadResponse(new BinaryReader(packet.Response)).Buffer;
        }

        public void Commit()
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TCommitCommand.WriteRequest(writer);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
        }

        public void Close()
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);
            TCloseCommand.WriteRequest(writer);

            TPacket packet = new TPacket(ms);
            Client.Send(packet);
        }

        public byte[] Tag
        {
            get
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);
                TGetTagCommand.WriteRequest(writer);

                TPacket packet = new TPacket(ms);
                Client.Send(packet);
                packet.Wait();

                return TGetTagCommand.ReadResponse(new BinaryReader(packet.Response)).Tag;
            }
            set
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);
                TSetTagCommand.WriteRequest(writer, value);

                TPacket packet = new TPacket(ms);
                Client.Send(packet);
            }
        }

        public long DataSize
        {
            get
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);
                TDataBaseSizeCommand.WriteRequest(writer);

                TPacket packet = new TPacket(ms);
                Client.Send(packet);
                packet.Wait();

                return TDataBaseSizeCommand.ReadResponse(new BinaryReader(packet.Response)).DataBaseSize;
            }
        }

        public long Size
        {
            get
            {
                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);
                TSizeCommand.WriteRequest(writer);

                TPacket packet = new TPacket(ms);
                Client.Send(packet);
                packet.Wait();

                return TSizeCommand.ReadResponse(new BinaryReader(packet.Response)).DataBaseSize;
            }
        }

        #endregion
    }
}