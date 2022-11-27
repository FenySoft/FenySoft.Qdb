using FenySoft.Qdb.WaterfallTree;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.Remote.Commands
{
    public class StorageEngineCommitCommand : ICommand
    {
        public StorageEngineCommitCommand()
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_COMMIT; }
        }
    }

    public class StorageEngineGetEnumeratorCommand : ICommand
    {
        public List<ITDescriptor> Descriptions;

        public StorageEngineGetEnumeratorCommand()
            : this(null)
        {
        }

        public StorageEngineGetEnumeratorCommand(List<ITDescriptor> descriptions)
        {
            Descriptions = descriptions;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_GET_ENUMERATOR; }
        }
    }

    public class StorageEngineRenameCommand : ICommand
    {
        public string Name;
        public string NewName;

        public StorageEngineRenameCommand(string name, string newName)
        {
            Name = name;
            NewName = newName;
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_RENAME; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class StorageEngineExistsCommand : ICommand
    {
        public string Name;
        public bool Exist;

        public StorageEngineExistsCommand(string name)
        {
            Name = name;
        }

        public StorageEngineExistsCommand(bool exist, string name)
        {
            Name = name;
            Exist = exist;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_EXISTS; }
        }
    }

    public class StorageEngineFindByIDCommand : ICommand
    {
        public ITDescriptor Descriptor;
        public long ID;

        public StorageEngineFindByIDCommand(ITDescriptor descriptor, long id)
        {
            Descriptor = descriptor;
            ID = id;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_FIND_BY_ID; }
        }
    }

    public class StorageEngineFindByNameCommand : ICommand
    {
        public string Name;
        public ITDescriptor Descriptor;

        public StorageEngineFindByNameCommand(string name, ITDescriptor descriptor)
        {
            Name = name;
            Descriptor = descriptor;
        }

        public StorageEngineFindByNameCommand(ITDescriptor descriptor)
            : this(null, descriptor)
        {
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_FIND_BY_NAME; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class StorageEngineOpenXIndexCommand : ICommand
    {
        public long ID;
        public string Name;

        public TDataType KeyType;
        public TDataType RecordType;

        public DateTime CreateTime;

        public StorageEngineOpenXIndexCommand(long id)
        {
            ID = id;
        }

        public StorageEngineOpenXIndexCommand(string name, TDataType keyType, TDataType recordType, DateTime createTime)
        {
            ID = -1;
            Name = name;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = createTime;
        }

        public StorageEngineOpenXIndexCommand(string name, TDataType keyType, TDataType recordType)
            : this(name, keyType, recordType, new DateTime())
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_OPEN_XTABLE; }
        }
    }

    public class StorageEngineOpenXFileCommand : ICommand
    {
        public long ID;
        public string Name;

        public StorageEngineOpenXFileCommand(string name)
        {
            Name = name;
        }

        public StorageEngineOpenXFileCommand(long id)
        {
            ID = id;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_OPEN_XFILE; }
        }
    }

    public class StorageEngineDeleteCommand : ICommand
    {
        public string Name;

        public StorageEngineDeleteCommand(string name)
        {
            Name = name;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_DELETE; }
        }
    }

    public class StorageEngineCountCommand : ICommand
    {
        public int Count;

        public StorageEngineCountCommand()
            : this(0)
        {
        }

        public StorageEngineCountCommand(int count)
        {
            Count = count;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_COUNT; }
        }
    }

    public class StorageEngineDescriptionCommand : ICommand
    {
        public ITDescriptor Descriptor;

        public StorageEngineDescriptionCommand(ITDescriptor description)
        {
            Descriptor = description;
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_DESCRIPTOR; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class StorageEngineGetCacheSizeCommand : ICommand
    {
        public int CacheSize;

        public StorageEngineGetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class StorageEngineSetCacheSizeCommand : ICommand
    {
        public int CacheSize;

        public StorageEngineSetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code
        {
            get { return CommandCode.STORAGE_ENGINE_SET_CACHE_SIZE; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class ExceptionCommand : ICommand
    {
        public readonly string Exception;

        public ExceptionCommand(string exception)
        {
            Exception = exception;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.EXCEPTION; }
        }
    }
}