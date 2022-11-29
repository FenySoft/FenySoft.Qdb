using FenySoft.Core.Persist;
using FenySoft.Core.Data;

namespace FenySoft.Qdb.Remote.Commands
{
    public class TCommandCollectionPersist : ITCommandCollectionPersist
    {
        public ITPersist<ITCommand> Persist { get; private set; }

        public TCommandCollectionPersist(ITPersist<ITCommand> persist)
        {
            Persist = persist;
        }

        public void Write(BinaryWriter writer, TCommandCollection collection)
        {
            int collectionCount = collection.Count;
            int commonAction = collection.CommonAction;

            writer.Write(collectionCount);
            writer.Write(commonAction);

            if (collectionCount > 1 && commonAction > 0)
            {
                switch (commonAction)
                {
                    case TCommandCode.REPLACE:
                    case TCommandCode.INSERT_OR_IGNORE:
                    case TCommandCode.DELETE:
                    case TCommandCode.DELETE_RANGE:
                    case TCommandCode.CLEAR:
                        {
                            for (int i = 0; i < collectionCount; i++)
                                Persist.Write(writer, collection[i]);
                        }
                        break;

                    default:
                        throw new NotImplementedException("Command is not implemented");
                }
            }
            else
            {
                foreach (var command in collection)
                    Persist.Write(writer, command);
            }
        }

        public TCommandCollection Read(BinaryReader reader)
        {
            int collectionCount = reader.ReadInt32();
            int commonAction = reader.ReadInt32();

            TCommandCollection collection = new TCommandCollection(collectionCount);

            if (collectionCount > 1 && commonAction > 0)
            {
                switch (commonAction)
                {
                    case TCommandCode.REPLACE:
                    case TCommandCode.INSERT_OR_IGNORE:
                    case TCommandCode.DELETE:
                    case TCommandCode.DELETE_RANGE:
                    case TCommandCode.CLEAR:
                        {
                            for (int i = 0; i < collectionCount; i++)
                                collection.Add(Persist.Read(reader));
                        }
                        break;

                    default:
                        throw new NotImplementedException("Command is not implemented");
                }
            }
            else
            {
                for (int i = 0; i < collectionCount; i++)
                    collection.Add(Persist.Read(reader));
            }

            return collection;
        }
    }

    public partial class TCommandPersist : ITPersist<ITCommand>
    {
        private Action<BinaryWriter, ITCommand>[] writes;
        private Func<BinaryReader, ITCommand>[] reads;

        public ITPersist<ITData> KeyPersist { get; private set; }
        public ITPersist<ITData> RecordPersist { get; private set; }

        public TCommandPersist(ITPersist<ITData> keyPersist, ITPersist<ITData> recordPersist)
        {
            KeyPersist = keyPersist;
            RecordPersist = recordPersist;

            // TXTable writers
            writes = new Action<BinaryWriter, ITCommand>[TCommandCode.MAX];
            writes[TCommandCode.REPLACE] = WriteReplaceCommand;
            writes[TCommandCode.DELETE] = WriteDeleteCommand;
            writes[TCommandCode.DELETE_RANGE] = WriteDeleteRangeCommand;
            writes[TCommandCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreCommand;
            writes[TCommandCode.CLEAR] = WriteClearCommand;
            writes[TCommandCode.TRY_GET] = WriteTryGetCommand;
            writes[TCommandCode.FORWARD] = WriteForwardCommand;
            writes[TCommandCode.BACKWARD] = WriteBackwardCommand;
            writes[TCommandCode.FIND_NEXT] = WriteFindNextCommand;
            writes[TCommandCode.FIND_AFTER] = WriteFindAfterCommand;
            writes[TCommandCode.FIND_PREV] = WriteFindPrevCommand;
            writes[TCommandCode.FIND_BEFORE] = WriteFindBeforeCommand;
            writes[TCommandCode.FIRST_ROW] = WriteFirstRowCommand;
            writes[TCommandCode.LAST_ROW] = WriteLastRowCommand;
            writes[TCommandCode.COUNT] = WriteCountCommand;
            writes[TCommandCode.XTABLE_DESCRIPTOR_GET] = WriteXIndexDescriptorGetCommand;
            writes[TCommandCode.XTABLE_DESCRIPTOR_SET] = WriteXIndexDescriptorSetCommand;

            // TXTable reads
            reads = new Func<BinaryReader, ITCommand>[TCommandCode.MAX];
            reads[TCommandCode.REPLACE] = ReadReplaceCommand;
            reads[TCommandCode.DELETE] = ReadDeleteCommand;
            reads[TCommandCode.DELETE_RANGE] = ReadDeleteRangeCommand;
            reads[TCommandCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreCommand;
            reads[TCommandCode.CLEAR] = ReadClearCommand;
            reads[TCommandCode.TRY_GET] = ReadTryGetCommand;
            reads[TCommandCode.FORWARD] = ReadForwardCommand;
            reads[TCommandCode.BACKWARD] = ReadBackwardCommand;
            reads[TCommandCode.FIND_NEXT] = ReadFindNextCommand;
            reads[TCommandCode.FIND_AFTER] = ReadFindAfterCommand;
            reads[TCommandCode.FIND_PREV] = ReadFindPrevCommand;
            reads[TCommandCode.FIND_BEFORE] = ReadFindBeforeCommand;
            reads[TCommandCode.FIRST_ROW] = ReadFirstRowCommand;
            reads[TCommandCode.LAST_ROW] = ReadLastRowCommand;
            reads[TCommandCode.COUNT] = ReadCountCommand;
            reads[TCommandCode.XTABLE_DESCRIPTOR_GET] = ReadXIndexDescriptorGetCommand;
            reads[TCommandCode.XTABLE_DESCRIPTOR_SET] = ReadXIndexDescriptorSetCommand;

            // Storage engine writes
            writes[TCommandCode.STORAGE_ENGINE_COMMIT] = WriteStorageEngineCommitCommand;
            writes[TCommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = WriteStorageEngineGetEnumeratorCommand;
            writes[TCommandCode.STORAGE_ENGINE_RENAME] = WriteStorageEngineRenameCommand;
            writes[TCommandCode.STORAGE_ENGINE_EXISTS] = WriteStorageEngineExistCommand;
            writes[TCommandCode.STORAGE_ENGINE_FIND_BY_ID] = WriteStorageEngineFindByIDCommand;
            writes[TCommandCode.STORAGE_ENGINE_FIND_BY_NAME] = WriteStorageEngineFindByNameCommand;
            writes[TCommandCode.STORAGE_ENGINE_OPEN_XTABLE] = WriteStorageEngineOpenXIndexCommand;
            writes[TCommandCode.STORAGE_ENGINE_OPEN_XFILE] = WriteStorageEngineOpenXFileCommand;
            writes[TCommandCode.STORAGE_ENGINE_DELETE] = WriteStorageEngineDeleteCommand;
            writes[TCommandCode.STORAGE_ENGINE_COUNT] = WriteStorageEngineCountCommand;
            writes[TCommandCode.STORAGE_ENGINE_DESCRIPTOR] = WriteStorageEngineDescriptionCommand;
            writes[TCommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = WriteStorageEngineGetCacheCommand;
            writes[TCommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = WriteStorageEngineSetCacheCommand;

            // Storage engine reads
            reads[TCommandCode.STORAGE_ENGINE_COMMIT] = ReadStorageEngineCommitCommand;
            reads[TCommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = ReadStorageEngineGetEnumeratorCommand;
            reads[TCommandCode.STORAGE_ENGINE_RENAME] = ReadStorageEngineRenameCommand;
            reads[TCommandCode.STORAGE_ENGINE_EXISTS] = ReadStorageEngineExistCommand;
            reads[TCommandCode.STORAGE_ENGINE_FIND_BY_ID] = ReadStorageEngineFindByIDCommand;
            reads[TCommandCode.STORAGE_ENGINE_FIND_BY_NAME] = ReadStorageEngineFindByNameCommand;
            reads[TCommandCode.STORAGE_ENGINE_OPEN_XTABLE] = ReadStorageEngineOpenXIndexCommand;
            reads[TCommandCode.STORAGE_ENGINE_OPEN_XFILE] = ReadStorageEngineOpenXFileCommand;
            reads[TCommandCode.STORAGE_ENGINE_DELETE] = ReadStorageEngineDeleteCommand;
            reads[TCommandCode.STORAGE_ENGINE_COUNT] = ReadStorageEngineCountCommand;
            reads[TCommandCode.STORAGE_ENGINE_DESCRIPTOR] = ReadStorageEngineDescriptionCommand;
            reads[TCommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = ReadStorageEngineGetCacheSizeCommand;
            reads[TCommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = ReadStorageEngineSetCacheCommand;

            //THeap writes
            writes[TCommandCode.HEAP_OBTAIN_NEW_HANDLE] = WriteHeapObtainNewHandleCommand;
            writes[TCommandCode.HEAP_RELEASE_HANDLE] = WriteHeapReleaseHandleCommand;
            writes[TCommandCode.HEAP_EXISTS_HANDLE] = WriteHeapExistsHandleCommand;
            writes[TCommandCode.HEAP_WRITE] = WriteHeapWriteCommand;
            writes[TCommandCode.HEAP_READ] = WriteHeapReadCommand;
            writes[TCommandCode.HEAP_COMMIT] = WriteHeapCommitCommand;
            writes[TCommandCode.HEAP_CLOSE] = WriteHeapCloseCommand;
            writes[TCommandCode.HEAP_SET_TAG] = WriteHeapSetTagCommand;
            writes[TCommandCode.HEAP_GET_TAG] = WriteHeapGetTagCommand;
            writes[TCommandCode.HEAP_DATA_SIZE] = WriteHeapDataSizeCommand;
            writes[TCommandCode.HEAP_SIZE] = WriteHeapSizeCommand;

            //THeap reads
            reads[TCommandCode.HEAP_OBTAIN_NEW_HANDLE] = ReadHeapObtainNewHandleCommand;
            reads[TCommandCode.HEAP_RELEASE_HANDLE] = ReadHeapReleaseHandleCommand;
            reads[TCommandCode.HEAP_EXISTS_HANDLE] = ReadHeapExistsHandleCommand;
            reads[TCommandCode.HEAP_WRITE] = ReadHeapWriteCommand;
            reads[TCommandCode.HEAP_READ] = ReadHeapReadCommand;
            reads[TCommandCode.HEAP_COMMIT] = ReadHeapCommitCommand;
            reads[TCommandCode.HEAP_CLOSE] = ReadHeapCloseCommand;
            reads[TCommandCode.HEAP_SET_TAG] = ReadHeapSetTagCommand;
            reads[TCommandCode.HEAP_GET_TAG] = ReadHeapGetTagCommand;
            reads[TCommandCode.HEAP_DATA_SIZE] = ReadHeapDataSizeCommand;
            reads[TCommandCode.HEAP_SIZE] = ReadHeapSizeCommand;

            writes[TCommandCode.EXCEPTION] = WriteExceptionCommand;
            reads[TCommandCode.EXCEPTION] = ReadExceptionCommand;
        }

        public void Write(BinaryWriter writer, ITCommand item)
        {
            writer.Write(item.Code);
            writes[item.Code](writer, item);
        }

        public ITCommand Read(BinaryReader reader)
        {
            int code = reader.ReadInt32();

            return reads[code](reader);
        }
    }
}