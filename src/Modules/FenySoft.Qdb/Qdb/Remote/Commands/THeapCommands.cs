namespace FenySoft.Qdb.Remote.Commands
{
    public class THeapObtainNewHandleCommand : ITCommand
    {
        public long Handle;

        public THeapObtainNewHandleCommand(long handle)
        {
            Handle = handle;
        }

        public THeapObtainNewHandleCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_OBTAIN_NEW_HANDLE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapReleaseHandleCommand : ITCommand
    {
        public long Handle;

        public THeapReleaseHandleCommand(long handle)
        {
            Handle = handle;
        }

        public int Code { get { return TCommandCode.HEAP_RELEASE_HANDLE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapExistsHandleCommand : ITCommand
    {
        public long Handle;
        public bool Exist;

        public THeapExistsHandleCommand(long handle, bool exist)
        {
            Handle = handle;
            Exist = exist;
        }

        public THeapExistsHandleCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_EXISTS_HANDLE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapWriteCommand : ITCommand
    {
        public long Handle;

        public byte[] Buffer;
        public int Index;
        public int Count;

        public THeapWriteCommand(long handle, byte[] buffer, int index, int count)
        {
            Handle = handle;
            Buffer = buffer;

            Index = index;
            Count = count;
        }

        public THeapWriteCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_WRITE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapReadCommand : ITCommand
    {
        public long Handle;
        public byte[] Buffer;

        public THeapReadCommand(long handle, byte[] buffer)
        {
            Handle = handle;
            Buffer = buffer;
        }

        public int Code { get { return TCommandCode.HEAP_READ; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapCommitCommand : ITCommand
    {
        public THeapCommitCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_COMMIT; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapCloseCommand : ITCommand
    {
        public THeapCloseCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_CLOSE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapGetTagCommand : ITCommand
    {
        public byte[] Tag;

        public THeapGetTagCommand(byte[] tag)
        {
            Tag = tag;
        }

        public THeapGetTagCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_GET_TAG; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapSetTagCommand : ITCommand
    {
        public byte[] Buffer;

        public THeapSetTagCommand(byte[] buffer)
        {
            Buffer = buffer;
        }

        public THeapSetTagCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_SET_TAG; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapDataSizeCommand : ITCommand
    {
        public long DataSize;

        public THeapDataSizeCommand(long dataSize)
        {
            DataSize = dataSize;
        }

        public THeapDataSizeCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_DATA_SIZE; } }

        public bool IsSynchronous { get { return true; } }
    }

    public class THeapSizeCommand : ITCommand
    {
        public long Size;

        public THeapSizeCommand(long size)
        {
            Size = size;
        }

        public THeapSizeCommand()
        {
        }

        public int Code { get { return TCommandCode.HEAP_SIZE; } }

        public bool IsSynchronous { get { return true; } }
    }
}
