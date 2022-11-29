using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;
using FenySoft.Qdb.Database;
using FenySoft.Qdb.Remote.Commands;
using FenySoft.Remote;

namespace FenySoft.Qdb.Remote
{
    public class TStorageEngineServer
    {
        private CancellationTokenSource ShutdownTokenSource;
        private Thread Worker;

        private Func<TXTablePortable, ITCommand, ITCommand>[] CommandsIIndexExecute;
        private Func<ITCommand, ITCommand>[] CommandsStorageEngineExecute;
        private Func<ITCommand, ITCommand>[] CommandsHeapExecute;

        public readonly ITStorageEngine StorageEngine;
        public readonly TTcpServer TcpServer;

        public TStorageEngineServer(ITStorageEngine storageEngine, TTcpServer tcpServer)
        {
            if (storageEngine == null)
                throw new ArgumentNullException("storageEngine");
            if (tcpServer == null)
                throw new ArgumentNullException("tcpServer");

            StorageEngine = storageEngine;
            TcpServer = tcpServer;

            CommandsIIndexExecute = new Func<TXTablePortable, ITCommand, ITCommand>[TCommandCode.MAX];
            CommandsIIndexExecute[TCommandCode.REPLACE] = Replace;
            CommandsIIndexExecute[TCommandCode.DELETE] = Delete;
            CommandsIIndexExecute[TCommandCode.DELETE_RANGE] = DeleteRange;
            CommandsIIndexExecute[TCommandCode.INSERT_OR_IGNORE] = InsertOrIgnore;
            CommandsIIndexExecute[TCommandCode.CLEAR] = Clear;
            CommandsIIndexExecute[TCommandCode.TRY_GET] = TryGet;
            CommandsIIndexExecute[TCommandCode.FORWARD] = Forward;
            CommandsIIndexExecute[TCommandCode.BACKWARD] = Backward;
            CommandsIIndexExecute[TCommandCode.FIND_NEXT] = FindNext;
            CommandsIIndexExecute[TCommandCode.FIND_AFTER] = FindAfter;
            CommandsIIndexExecute[TCommandCode.FIND_PREV] = FindPrev;
            CommandsIIndexExecute[TCommandCode.FIND_BEFORE] = FindBefore;
            CommandsIIndexExecute[TCommandCode.FIRST_ROW] = FirstRow;
            CommandsIIndexExecute[TCommandCode.LAST_ROW] = LastRow;
            CommandsIIndexExecute[TCommandCode.COUNT] = Count;
            CommandsIIndexExecute[TCommandCode.XTABLE_DESCRIPTOR_GET] = GetXIndexDescriptor;
            CommandsIIndexExecute[TCommandCode.XTABLE_DESCRIPTOR_SET] = SetXIndexDescriptor;

            CommandsStorageEngineExecute = new Func<ITCommand, ITCommand>[TCommandCode.MAX];
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_COMMIT] = StorageEngineCommit;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_GET_ENUMERATOR] = StorageEngineGetEnumerator;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_RENAME] = StorageEngineRename;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_EXISTS] = StorageEngineExist;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_FIND_BY_ID] = StorageEngineFindByID;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_FIND_BY_NAME] = StorageEngineFindByNameCommand;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_OPEN_XTABLE] = StorageEngineOpenXIndex;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_OPEN_XFILE] = StorageEngineOpenXFile;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_DELETE] = StorageEngineDelete;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_COUNT] = StorageEngineCount;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_GET_CACHE_SIZE] = StorageEngineGetCacheSize;
            CommandsStorageEngineExecute[TCommandCode.STORAGE_ENGINE_SET_CACHE_SIZE] = StorageEngineSetCacheSize;
            CommandsStorageEngineExecute[TCommandCode.HEAP_OBTAIN_NEW_HANDLE] = HeapObtainNewHandle;
            CommandsStorageEngineExecute[TCommandCode.HEAP_RELEASE_HANDLE] = HeapReleaseHandle;
            CommandsStorageEngineExecute[TCommandCode.HEAP_EXISTS_HANDLE] = HeapExistsHandle;
            CommandsStorageEngineExecute[TCommandCode.HEAP_WRITE] = HeapWrite;
            CommandsStorageEngineExecute[TCommandCode.HEAP_READ] = HeapRead;
            CommandsStorageEngineExecute[TCommandCode.HEAP_COMMIT] = HeapCommit;
            CommandsStorageEngineExecute[TCommandCode.HEAP_CLOSE] = HeapClose;
            CommandsStorageEngineExecute[TCommandCode.HEAP_GET_TAG] = HeapGetTag;
            CommandsStorageEngineExecute[TCommandCode.HEAP_SET_TAG] = HeapSetTag;
            CommandsStorageEngineExecute[TCommandCode.HEAP_DATA_SIZE] = HeapDataSize;
            CommandsStorageEngineExecute[TCommandCode.HEAP_SIZE] = HeapSize;
        }

        public void Start()
        {
            Stop();

            ShutdownTokenSource = new CancellationTokenSource();

            Worker = new Thread(DoWork);
            Worker.Start();
        }

        public void Stop()
        {
            if (!IsWorking)
                return;

            ShutdownTokenSource.Cancel(false);

            Thread thread = Worker;
            if (thread != null)
            {
                if (!thread.Join(5000))
                    thread.Abort();
            }
        }

        public bool IsWorking
        {
            get { return Worker != null; }
        }

        private void DoWork()
        {
            try
            {
                TcpServer.Start();

                while (!ShutdownTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        KeyValuePair<TServerConnection, TPacket> order = TcpServer.RecievedPacketsTake(ShutdownTokenSource.Token);
                        Task.Factory.StartNew(PacketExecute, order);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception exc)
                    {
                        TcpServer.LogError(exc);
                    }
                }
            }
            catch (Exception exc)
            {
                TcpServer.LogError(exc);
            }
            finally
            {
                TcpServer.Stop();

                Worker = null;
            }
        }

        private void PacketExecute(object state)
        {
            try
            {
                KeyValuePair<TServerConnection, TPacket> order = (KeyValuePair<TServerConnection, TPacket>)state;

                BinaryReader reader = new BinaryReader(order.Value.Request);
                TMessage msgRequest = TMessage.Deserialize(reader, (id) => StorageEngine.Find(id));

                ITDescriptor clientDescription = msgRequest.Description;
                TCommandCollection resultCommands = new TCommandCollection(1);

                try
                {
                    var commands = msgRequest.Commands;

                    if (msgRequest.Description != null) // TXTable commands
                    {
                        TXTablePortable table = (TXTablePortable)StorageEngine.OpenXTablePortable(clientDescription.Name, clientDescription.KeyDataType, clientDescription.RecordDataType);
                        table.Descriptor.Tag = clientDescription.Tag;

                        for (int i = 0; i < commands.Count - 1; i++)
                        {
                            ITCommand command = msgRequest.Commands[i];
                            CommandsIIndexExecute[command.Code](table, command);
                        }

                        ITCommand resultCommand = CommandsIIndexExecute[msgRequest.Commands[commands.Count - 1].Code](table, msgRequest.Commands[commands.Count - 1]);
                        if (resultCommand != null)
                            resultCommands.Add(resultCommand);

                        table.Flush();
                    }
                    else //Storage engine commands
                    {
                        ITCommand command = msgRequest.Commands[commands.Count - 1];

                        var resultCommand = CommandsStorageEngineExecute[command.Code](command);

                        if (resultCommand != null)
                            resultCommands.Add(resultCommand);
                    }
                }
                catch (Exception e)
                {
                    resultCommands.Add(new TExceptionCommand(e.Message));
                }

                MemoryStream ms = new MemoryStream();
                BinaryWriter writer = new BinaryWriter(ms);

                TDescriptor responseClientDescription = new TDescriptor(-1, "", TStructureType.RESERVED, TDataType.Boolean, TDataType.Boolean, null, null, DateTime.Now, DateTime.Now, DateTime.Now, null);

                TMessage msgResponse = new TMessage(msgRequest.Description == null ? responseClientDescription : msgRequest.Description, resultCommands);
                msgResponse.Serialize(writer);

                ms.Position = 0;
                order.Value.Response = ms;
                order.Key.FPendingPackets.Add(order.Value);
            }
            catch (Exception exc)
            {
                TcpServer.LogError(exc);
            }
        }

        #region TXTable Commands

        private ITCommand Replace(TXTablePortable table, ITCommand command)
        {
            TReplaceCommand cmd = (TReplaceCommand)command;
            table.Replace(cmd.Key, cmd.Record);

            return null;
        }

        private ITCommand Delete(TXTablePortable table, ITCommand command)
        {
            TDeleteCommand cmd = (TDeleteCommand)command;
            table.Delete(cmd.Key);

            return null;
        }

        private ITCommand DeleteRange(TXTablePortable table, ITCommand command)
        {
            TDeleteRangeCommand cmd = (TDeleteRangeCommand)command;
            table.Delete(cmd.FromKey, cmd.ToKey);

            return null;
        }

        private ITCommand InsertOrIgnore(TXTablePortable table, ITCommand command)
        {
            TInsertOrIgnoreCommand cmd = (TInsertOrIgnoreCommand)command;
            table.InsertOrIgnore(cmd.Key, cmd.Record);

            return null;
        }

        private ITCommand Clear(TXTablePortable table, ITCommand command)
        {
            table.Clear();

            return null;
        }

        private ITCommand TryGet(TXTablePortable table, ITCommand command)
        {
            TTryGetCommand cmd = (TTryGetCommand)command;
            ITData record = null;

            bool exist = table.TryGet(cmd.Key, out record);

            return new TTryGetCommand(cmd.Key, record);
        }

        private ITCommand Forward(TXTablePortable table, ITCommand command)
        {
            TForwardCommand cmd = (TForwardCommand)command;

            List<KeyValuePair<ITData, ITData>> list = table.Forward(cmd.FromKey, cmd.FromKey != null, cmd.ToKey, cmd.ToKey != null).Take(cmd.PageCount).ToList();

            return new TForwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
        }

        private ITCommand Backward(TXTablePortable table, ITCommand command)
        {
            TBackwardCommand cmd = (TBackwardCommand)command;

            List<KeyValuePair<ITData, ITData>> list = table.Backward(cmd.FromKey, cmd.FromKey != null, cmd.ToKey, cmd.ToKey != null).Take(cmd.PageCount).ToList();

            return new TBackwardCommand(cmd.PageCount, cmd.FromKey, cmd.ToKey, list);
        }

        private ITCommand FindNext(TXTablePortable table, ITCommand command)
        {
            TFindNextCommand cmd = (TFindNextCommand)command;
            KeyValuePair<ITData, ITData>? keyValue = table.FindNext(cmd.Key);

            return new TFindNextCommand(cmd.Key, keyValue);
        }

        private ITCommand FindAfter(TXTablePortable table, ITCommand command)
        {
            TFindAfterCommand cmd = (TFindAfterCommand)command;
            KeyValuePair<ITData, ITData>? keyValue = table.FindAfter(cmd.Key);

            return new TFindAfterCommand(cmd.Key, keyValue);
        }

        private ITCommand FindPrev(TXTablePortable table, ITCommand command)
        {
            TFindPrevCommand cmd = (TFindPrevCommand)command;
            KeyValuePair<ITData, ITData>? keyValue = table.FindPrev(cmd.Key);

            return new TFindPrevCommand(cmd.Key, keyValue);
        }

        private ITCommand FindBefore(TXTablePortable table, ITCommand command)
        {
            TFindBeforeCommand cmd = (TFindBeforeCommand)command;
            KeyValuePair<ITData, ITData>? keyValue = table.FindBefore(cmd.Key);

            return new TFindBeforeCommand(cmd.Key, keyValue);
        }

        private ITCommand FirstRow(TXTablePortable table, ITCommand command)
        {
            KeyValuePair<ITData, ITData> cmd = table.FirstRow;

            return new TFirstRowCommand(cmd);
        }

        private ITCommand LastRow(TXTablePortable table, ITCommand command)
        {
            KeyValuePair<ITData, ITData> cmd = table.LastRow;

            return new TLastRowCommand(cmd);
        }

        private ITCommand GetXIndexDescriptor(TXTablePortable table, ITCommand command)
        {
            TXTableDescriptorGetCommand cmd = (TXTableDescriptorGetCommand)command;
            cmd.Descriptor = table.Descriptor;

            return new TXTableDescriptorGetCommand(cmd.Descriptor);
        }

        private ITCommand SetXIndexDescriptor(TXTablePortable table, ITCommand command)
        {
            TXTableDescriptorSetCommand cmd = (TXTableDescriptorSetCommand)command;
            TDescriptor descriptor = (TDescriptor)cmd.Descriptor;

            if (descriptor.Tag != null)
                table.Descriptor.Tag = descriptor.Tag;

            return new TXTableDescriptorSetCommand(descriptor);
        }

        #endregion

        #region TStorageEngine Commands

        private ITCommand Count(TXTablePortable table, ITCommand command)
        {
            long count = table.Count();

            return new TCountCommand(count);
        }

        private ITCommand StorageEngineCommit(ITCommand command)
        {
            StorageEngine.Commit();

            return new TStorageEngineCommitCommand();
        }

        private ITCommand StorageEngineGetEnumerator(ITCommand command)
        {
            List<ITDescriptor> list = new List<ITDescriptor>();

            foreach (var locator in StorageEngine)
                list.Add(new TDescriptor(locator.ID, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag));

            return new TStorageEngineGetEnumeratorCommand(list);
        }

        private ITCommand StorageEngineExist(ITCommand command)
        {
            TStorageEngineExistsCommand cmd = (TStorageEngineExistsCommand)command;
            bool exist = StorageEngine.Exists(cmd.Name);

            return new TStorageEngineExistsCommand(exist, cmd.Name);
        }

        private ITCommand StorageEngineFindByID(ITCommand command)
        {
            TStorageEngineFindByIDCommand cmd = (TStorageEngineFindByIDCommand)command;

            ITDescriptor locator = StorageEngine.Find(cmd.ID);

            return new TStorageEngineFindByIDCommand(new TDescriptor(locator.ID, locator.Name, locator.StructureType, locator.KeyDataType, locator.RecordDataType, locator.KeyType, locator.RecordType, locator.CreateTime, locator.ModifiedTime, locator.AccessTime, locator.Tag), cmd.ID);
        }

        private ITCommand StorageEngineFindByNameCommand(ITCommand command)
        {
            TStorageEngineFindByNameCommand cmd = (TStorageEngineFindByNameCommand)command;
            cmd.Descriptor = StorageEngine[cmd.Name];

            return new TStorageEngineFindByNameCommand(cmd.Name, cmd.Descriptor);
        }

        private ITCommand StorageEngineOpenXIndex(ITCommand command)
        {
            TStorageEngineOpenXIndexCommand cmd = (TStorageEngineOpenXIndexCommand)command;
            StorageEngine.OpenXTablePortable(cmd.Name, cmd.KeyType, cmd.RecordType);

            ITDescriptor locator = StorageEngine[cmd.Name];

            return new TStorageEngineOpenXIndexCommand(locator.ID);
        }

        private ITCommand StorageEngineOpenXFile(ITCommand command)
        {
            TStorageEngineOpenXFileCommand cmd = (TStorageEngineOpenXFileCommand)command;
            StorageEngine.OpenXFile(cmd.Name);

            ITDescriptor locator = StorageEngine[cmd.Name];

            return new TStorageEngineOpenXFileCommand(locator.ID);
        }

        private ITCommand StorageEngineDelete(ITCommand command)
        {
            TStorageEngineDeleteCommand cmd = (TStorageEngineDeleteCommand)command;
            StorageEngine.Delete(cmd.Name);

            return new TStorageEngineDeleteCommand(cmd.Name);
        }

        private ITCommand StorageEngineRename(ITCommand command)
        {
            TStorageEngineRenameCommand cmd = (TStorageEngineRenameCommand)command;
            StorageEngine.Rename(cmd.Name, cmd.NewName);

            return new TStorageEngineRenameCommand(cmd.Name, cmd.NewName);
        }

        private ITCommand StorageEngineCount(ITCommand command)
        {
            TStorageEngineCountCommand cmd = (TStorageEngineCountCommand)command;
            int count = StorageEngine.Count;

            return new TStorageEngineCountCommand(count);
        }

        private ITCommand StorageEngineGetCacheSize(ITCommand command)
        {
            int cacheSize = StorageEngine.CacheSize;

            return new TStorageEngineGetCacheSizeCommand(cacheSize);
        }

        private ITCommand StorageEngineSetCacheSize(ITCommand command)
        {
            TStorageEngineSetCacheSizeCommand cmd = (TStorageEngineSetCacheSizeCommand)command;
            StorageEngine.CacheSize = cmd.CacheSize;

            return new TStorageEngineGetCacheSizeCommand(cmd.CacheSize);
        }

        #endregion

        #region THeap Commands

        private ITCommand HeapObtainNewHandle(ITCommand command)
        {
            long handle = StorageEngine.Heap.ObtainNewHandle();

            return new THeapObtainNewHandleCommand(handle);
        }

        private ITCommand HeapReleaseHandle(ITCommand command)
        {
            THeapReleaseHandleCommand cmd = (THeapReleaseHandleCommand)command;
            StorageEngine.Heap.Release(cmd.Handle);

            return new THeapReleaseHandleCommand(-1);
        }

        public ITCommand HeapExistsHandle(ITCommand command)
        {
            THeapExistsHandleCommand cmd = (THeapExistsHandleCommand)command;
            var exists = StorageEngine.Heap.Exists(cmd.Handle);

            return new THeapExistsHandleCommand(cmd.Handle, exists);
        }

        public ITCommand HeapWrite(ITCommand command)
        {
            THeapWriteCommand cmd = (THeapWriteCommand)command;
            StorageEngine.Heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);

            return new THeapWriteCommand();
        }

        public ITCommand HeapRead(ITCommand command)
        {
            THeapReadCommand cmd = (THeapReadCommand)command;
            var buffer = StorageEngine.Heap.Read(cmd.Handle);

            return new THeapReadCommand(cmd.Handle, buffer);
        }

        public ITCommand HeapCommit(ITCommand command)
        {
            StorageEngine.Heap.Commit();

            return command;
        }

        public ITCommand HeapClose(ITCommand command)
        {
            StorageEngine.Heap.Close();

            return command;
        }

        public ITCommand HeapGetTag(ITCommand command)
        {
            var tag = StorageEngine.Heap.Tag;

            return new THeapGetTagCommand(tag);
        }

        public ITCommand HeapSetTag(ITCommand command)
        {
            THeapSetTagCommand cmd = (THeapSetTagCommand)command;

            StorageEngine.Heap.Tag = cmd.Buffer;

            return new THeapSetTagCommand();
        }

        public ITCommand HeapDataSize(ITCommand command)
        {
            var dataSize = StorageEngine.Heap.DataSize;

            return new THeapDataSizeCommand(dataSize);
        }

        public ITCommand HeapSize(ITCommand command)
        {
            var size = StorageEngine.Heap.Size;

            return new THeapSizeCommand(size);
        }
        #endregion
    }
}