using FenySoft.Qdb.WaterfallTree;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.Remote.Commands
{
    public class TStorageEngineCommitCommand : ITCommand
    {
        public TStorageEngineCommitCommand()
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_COMMIT; }
        }
    }

    public class TStorageEngineGetEnumeratorCommand : ITCommand
    {
        public List<ITDescriptor> Descriptions;

        public TStorageEngineGetEnumeratorCommand()
            : this(null)
        {
        }

        public TStorageEngineGetEnumeratorCommand(List<ITDescriptor> descriptions)
        {
            Descriptions = descriptions;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_GET_ENUMERATOR; }
        }
    }

    public class TStorageEngineRenameCommand : ITCommand
    {
        public string Name;
        public string NewName;

        public TStorageEngineRenameCommand(string name, string newName)
        {
            Name = name;
            NewName = newName;
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_RENAME; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TStorageEngineExistsCommand : ITCommand
    {
        public string Name;
        public bool Exist;

        public TStorageEngineExistsCommand(string name)
        {
            Name = name;
        }

        public TStorageEngineExistsCommand(bool exist, string name)
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
            get { return TCommandCode.STORAGE_ENGINE_EXISTS; }
        }
    }

    public class TStorageEngineFindByIDCommand : ITCommand
    {
        public ITDescriptor Descriptor;
        public long ID;

        public TStorageEngineFindByIDCommand(ITDescriptor descriptor, long id)
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
            get { return TCommandCode.STORAGE_ENGINE_FIND_BY_ID; }
        }
    }

    public class TStorageEngineFindByNameCommand : ITCommand
    {
        public string Name;
        public ITDescriptor Descriptor;

        public TStorageEngineFindByNameCommand(string name, ITDescriptor descriptor)
        {
            Name = name;
            Descriptor = descriptor;
        }

        public TStorageEngineFindByNameCommand(ITDescriptor descriptor)
            : this(null, descriptor)
        {
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_FIND_BY_NAME; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TStorageEngineOpenXIndexCommand : ITCommand
    {
        public long ID;
        public string Name;

        public TDataType KeyType;
        public TDataType RecordType;

        public DateTime CreateTime;

        public TStorageEngineOpenXIndexCommand(long id)
        {
            ID = id;
        }

        public TStorageEngineOpenXIndexCommand(string name, TDataType keyType, TDataType recordType, DateTime createTime)
        {
            ID = -1;
            Name = name;

            KeyType = keyType;
            RecordType = recordType;

            CreateTime = createTime;
        }

        public TStorageEngineOpenXIndexCommand(string name, TDataType keyType, TDataType recordType)
            : this(name, keyType, recordType, new DateTime())
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_OPEN_XTABLE; }
        }
    }

    public class TStorageEngineOpenXFileCommand : ITCommand
    {
        public long ID;
        public string Name;

        public TStorageEngineOpenXFileCommand(string name)
        {
            Name = name;
        }

        public TStorageEngineOpenXFileCommand(long id)
        {
            ID = id;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_OPEN_XFILE; }
        }
    }

    public class TStorageEngineDeleteCommand : ITCommand
    {
        public string Name;

        public TStorageEngineDeleteCommand(string name)
        {
            Name = name;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_DELETE; }
        }
    }

    public class TStorageEngineCountCommand : ITCommand
    {
        public int Count;

        public TStorageEngineCountCommand()
            : this(0)
        {
        }

        public TStorageEngineCountCommand(int count)
        {
            Count = count;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_COUNT; }
        }
    }

    public class TStorageEngineDescriptionCommand : ITCommand
    {
        public ITDescriptor Descriptor;

        public TStorageEngineDescriptionCommand(ITDescriptor description)
        {
            Descriptor = description;
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_DESCRIPTOR; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TStorageEngineGetCacheSizeCommand : ITCommand
    {
        public int CacheSize;

        public TStorageEngineGetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_GET_CACHE_SIZE; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TStorageEngineSetCacheSizeCommand : ITCommand
    {
        public int CacheSize;

        public TStorageEngineSetCacheSizeCommand(int cacheSize)
        {
            CacheSize = cacheSize;
        }

        public int Code
        {
            get { return TCommandCode.STORAGE_ENGINE_SET_CACHE_SIZE; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TExceptionCommand : ITCommand
    {
        public readonly string Exception;

        public TExceptionCommand(string exception)
        {
            Exception = exception;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.EXCEPTION; }
        }
    }
}