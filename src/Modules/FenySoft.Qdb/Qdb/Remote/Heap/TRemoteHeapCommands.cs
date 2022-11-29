using FenySoft.Core.Compression;

namespace FenySoft.Qdb.Remote.Heap
{
    public class THeapCommand
    {
    }

    public class TObtainHandleCommand : THeapCommand
    {
        public long Handle;

        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.ObtainHandle);
        }

        public static void WriteResponse(BinaryWriter writer, long handle)
        {
            TCountCompression.Serialize(writer, (ulong)handle);
        }

        public static TObtainHandleCommand ReadResponse(BinaryReader reader)
        {
            return new TObtainHandleCommand()
            {
                Handle = (long)TCountCompression.Deserialize(reader)
            };
        }
    }

    public class TReleaseHandleCommand : THeapCommand
    {
        public long Handle;

        public static void WriteRequest(BinaryWriter writer, long handle)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.ReleaseHandle);
            writer.Write(handle);
        }

        public static TReleaseHandleCommand ReadRequest(BinaryReader reader)
        {
            return new TReleaseHandleCommand()
            {
                Handle = (long)TCountCompression.Deserialize(reader)
            };
        }
    }

    public class THandleExistCommand : THeapCommand
    {
        public bool Exist;
        public long Handle;

        public static void WriteRequest(BinaryWriter writer, long handle)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.HandleExist);
            TCountCompression.Serialize(writer, (ulong)handle);
        }

        public static THandleExistCommand ReadRequest(BinaryReader reader)
        {
            return new THandleExistCommand()
            {
                Handle = (long)TCountCompression.Deserialize(reader)
            };
        }

        public static void WriteResponse(BinaryWriter writer, bool exist)
        {
            writer.Write(exist);
        }

        public static THandleExistCommand ReadResponse(BinaryReader reader)
        {
            return new THandleExistCommand()
            {
                Exist = reader.ReadBoolean()
            };
        }
    }

    public class TWriteCommand : THeapCommand
    {
        public long Handle;

        public int Index;
        public int Count;
        public byte[] Buffer;

        public static void WriteRequest(BinaryWriter writer, long handle, int index, int count, byte[] buffer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.WriteCommand);
            TCountCompression.Serialize(writer, (ulong)handle);

            TCountCompression.Serialize(writer, (ulong)index);
            TCountCompression.Serialize(writer, (ulong)count);

            TCountCompression.Serialize(writer, (ulong)(count + index));
            writer.Write(buffer, 0, index + count);
        }

        public static TWriteCommand ReadRequest(BinaryReader reader)
        {
            return new TWriteCommand()
            {
                Handle = (long)TCountCompression.Deserialize(reader),

                Index = (int)TCountCompression.Deserialize(reader),
                Count = (int)TCountCompression.Deserialize(reader),

                Buffer = reader.ReadBytes((int)TCountCompression.Deserialize(reader))
            };
        }
    }

    public class TReadCommand : THeapCommand
    {
        public long Handle;
        public byte[] Buffer;

        public static void WriteRequest(BinaryWriter writer, long handle)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.ReadCommand);
            TCountCompression.Serialize(writer, (ulong)handle);
        }

        public static TReadCommand ReadRequest(BinaryReader reader)
        {
            return new TReadCommand()
            {
                Handle = (long)TCountCompression.Deserialize(reader)
            };
        }

        public static void WriteResponse(BinaryWriter writer, byte[] buffer)
        {
            TCountCompression.Serialize(writer, (ulong)buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        public static TReadCommand ReadResponse(BinaryReader reader)
        {
            return new TReadCommand()
            {
                Buffer = reader.ReadBytes((int)TCountCompression.Deserialize(reader))
            };
        }
    }

    public class TCommitCommand : THeapCommand
    {
        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.CommitCommand);
        }
    }

    public class TCloseCommand : THeapCommand
    {
        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.CloseCommand);
        }
    }

    public class TSetTagCommand : THeapCommand
    {
        public byte[] Tag;

        public static void WriteRequest(BinaryWriter writer, byte[] tag)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.SetTag);
            writer.Write(tag != null);
            if (tag != null)
            {
                TCountCompression.Serialize(writer, (ulong)tag.Length);
                writer.Write(tag, 0, tag.Length);
            }
        }

        public static TSetTagCommand ReadRequest(BinaryReader reader)
        {
            return new TSetTagCommand()
            {
                Tag = reader.ReadBoolean() ? reader.ReadBytes((int)TCountCompression.Deserialize(reader)) : null
            };
        }
    }

    public class TGetTagCommand : THeapCommand
    {
        public byte[] Tag;

        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.GetTag);
        }

        public static void WriteResponse(BinaryWriter writer, byte[] tag)
        {
            writer.Write(tag != null);
            if (tag != null)
            {
                TCountCompression.Serialize(writer, (ulong)tag.Length);
                writer.Write(tag, 0, tag.Length);
            }
        }

        public static TGetTagCommand ReadResponse(BinaryReader reader)
        {
            return new TGetTagCommand()
            {
                Tag = reader.ReadBoolean() ? reader.ReadBytes((int)TCountCompression.Deserialize(reader)) : null
            };
        }
    }

    public class TDataBaseSizeCommand : THeapCommand
    {
        public long DataBaseSize;

        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.DataBaseSize);
        }

        public static void WriteResponse(BinaryWriter writer, long size)
        {
            TCountCompression.Serialize(writer, (ulong)size);
        }

        public static TDataBaseSizeCommand ReadResponse(BinaryReader reader)
        {
            return new TDataBaseSizeCommand()
             {
                 DataBaseSize = (long)TCountCompression.Deserialize(reader)
             };
        }
    }

    public class TSizeCommand : THeapCommand
    {
        public long Size;

        public static void WriteRequest(BinaryWriter writer)
        {
            writer.Write((byte)TRemoteHeapCommandCodes.Size);
        }

        public static void WriteResponse(BinaryWriter writer, long size)
        {
            TCountCompression.Serialize(writer, (ulong)size);
        }

        public static TDataBaseSizeCommand ReadResponse(BinaryReader reader)
        {
            return new TDataBaseSizeCommand()
            {
                DataBaseSize = (long)TCountCompression.Deserialize(reader)
            };
        }
    }

    public enum TRemoteHeapCommandCodes : byte
    {
        ObtainHandle = 1,
        ReleaseHandle = 2,
        HandleExist = 3,
        WriteCommand = 4,
        ReadCommand = 5,
        CommitCommand = 6,
        CloseCommand = 7,
        SetTag = 8,
        GetTag = 9,
        DataBaseSize = 10,
        Size = 11
    }
}
