using FenySoft.Core.Data;
using FenySoft.Qdb.Remote.Commands;
using FenySoft.Qdb.WaterfallTree;

using System.Collections;
using System.Collections.Concurrent;

using FenySoft.Qdb.Database;
using FenySoft.Remote;

namespace FenySoft.Qdb.Remote
{
    public class TStorageEngineClient : ITStorageEngine
    {
        private int cacheSize;
        private ConcurrentDictionary<string, TXTableRemote> indexes = new ConcurrentDictionary<string, TXTableRemote>();

        public static readonly TDescriptor StorageEngineDescriptor = new TDescriptor(-1, "", TDataType.Boolean, TDataType.Boolean);
        public readonly TClientConnection ClientConnection;

        public TStorageEngineClient(string machineName = "localhost", int port = 7182)
        {
            ClientConnection = new TClientConnection(machineName, port);
            ClientConnection.Start();

            Heap = new RemoteHeap(this);
        }

        #region ITStorageEngine

        public ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName, TDataType AKeyDataType, TDataType ARecordDataType, ITTransformer<TKey, ITData> AKeyTransformer, ITTransformer<TRecord, ITData> ARecordTransformer)
        {
            var index = OpenXTablePortable(AName, AKeyDataType, ARecordDataType);

            return new TXTablePortable<TKey, TRecord>(index, AKeyTransformer, ARecordTransformer);
        }

        public ITTable<ITData, ITData> OpenXTablePortable(string AName, TDataType AKeyDataType, TDataType ARecordDataType)
        {
            var cmd = new TStorageEngineOpenXIndexCommand(AName, AKeyDataType, ARecordDataType);
            InternalExecute(cmd);

            var descriptor = new TDescriptor(cmd.ID, AName, AKeyDataType, ARecordDataType);

            var index = new TXTableRemote(this, descriptor);
            indexes.TryAdd(AName, index);

            return index;
        }

        public ITTable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string AName)
        {
            var keyDataType = TDataTypeUtils.BuildDataType(typeof(TKey));
            var recordDataType = TDataTypeUtils.BuildDataType(typeof(TRecord));

            var keyTransformer = new TDataTransformer<TKey>(typeof(TKey));
            var recordTransformer = new TDataTransformer<TRecord>(typeof(TRecord));

            return OpenXTablePortable<TKey, TRecord>(AName, keyDataType, recordDataType, null, null);
        }

        public ITTable<TKey, TRecord> OpenXTable<TKey, TRecord>(string AName)
        {
            return OpenXTablePortable<TKey, TRecord>(AName);
        }

        public TXFile OpenXFile(string AName)
        {
            throw new NotSupportedException();
        }

        public void Rename(string AName, string ANewName)
        {
            InternalExecute(new TStorageEngineRenameCommand(AName, ANewName));
        }

        public ITDescriptor this[string AName]
        {
            get
            {
                return indexes[AName].Descriptor;
            }
        }

        public void Delete(string AName)
        {
            var cmd = new TStorageEngineDeleteCommand(AName);
            InternalExecute(cmd);
        }

        public bool Exists(string AName)
        {
            var cmd = new TStorageEngineExistsCommand(AName);
            InternalExecute(cmd);

            return cmd.Exist;
        }

        public int Count
        {
            get
            {
                var cmd = new TStorageEngineCountCommand();
                InternalExecute(cmd);

                return cmd.Count;
            }
        }

        public ITDescriptor Find(long AId)
        {
            var cmd = new TStorageEngineFindByIDCommand(null, AId);
            InternalExecute(cmd);

            return cmd.Descriptor;
        }

        public IEnumerator<ITDescriptor> GetEnumerator()
        {
            var cmd = new TStorageEngineGetEnumeratorCommand();
            InternalExecute(cmd);

            return cmd.Descriptions.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Commit()
        {
            foreach (var index in indexes.Values)
                index.Flush();

            InternalExecute(new TStorageEngineCommitCommand());
        }

        public ITHeap Heap { get; private set; }

        #endregion

        #region Server

        public TCommandCollection Execute(ITDescriptor descriptor, TCommandCollection commands)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            TMessage message = new TMessage(descriptor, commands);
            message.Serialize(writer);

            TPacket packet = new TPacket(ms);
            ClientConnection.Send(packet);

            packet.Wait();

            BinaryReader reader = new BinaryReader(packet.Response);
            message = TMessage.Deserialize(reader, (id) => { return descriptor; });

            return message.Commands;
        }

        private void InternalExecute(ITCommand command)
        {
            TCommandCollection cmds = new TCommandCollection(1);
            cmds.Add(command);

            var resultCommand = Execute(StorageEngineDescriptor, cmds)[0];
            SetResult(command, resultCommand);
        }

        private void SetResult(ITCommand command, ITCommand resultCommand)
        {
            switch (resultCommand.Code)
            {
                case TCommandCode.STORAGE_ENGINE_COMMIT:
                    break;

                case TCommandCode.STORAGE_ENGINE_OPEN_XTABLE:
                    {
                        ((TStorageEngineOpenXIndexCommand)command).ID = ((TStorageEngineOpenXIndexCommand)resultCommand).ID;
                        ((TStorageEngineOpenXIndexCommand)command).CreateTime = ((TStorageEngineOpenXIndexCommand)resultCommand).CreateTime;
                    }
                    break;

                case TCommandCode.STORAGE_ENGINE_OPEN_XFILE:
                    ((TStorageEngineOpenXFileCommand)command).ID = ((TStorageEngineOpenXFileCommand)resultCommand).ID;
                    break;

                case TCommandCode.STORAGE_ENGINE_EXISTS:
                    ((TStorageEngineExistsCommand)command).Exist = ((TStorageEngineExistsCommand)resultCommand).Exist;
                    break;

                case TCommandCode.STORAGE_ENGINE_FIND_BY_ID:
                    ((TStorageEngineFindByIDCommand)command).Descriptor = ((TStorageEngineFindByIDCommand)resultCommand).Descriptor;
                    break;

                case TCommandCode.STORAGE_ENGINE_FIND_BY_NAME:
                    ((TStorageEngineFindByNameCommand)command).Descriptor = ((TStorageEngineFindByNameCommand)resultCommand).Descriptor;
                    break;

                case TCommandCode.STORAGE_ENGINE_DELETE:
                    break;

                case TCommandCode.STORAGE_ENGINE_COUNT:
                    ((TStorageEngineCountCommand)command).Count = ((TStorageEngineCountCommand)resultCommand).Count;
                    break;

                case TCommandCode.STORAGE_ENGINE_GET_ENUMERATOR:
                    ((TStorageEngineGetEnumeratorCommand)command).Descriptions = ((TStorageEngineGetEnumeratorCommand)resultCommand).Descriptions;
                    break;

                case TCommandCode.STORAGE_ENGINE_GET_CACHE_SIZE:
                    ((TStorageEngineGetCacheSizeCommand)command).CacheSize = ((TStorageEngineGetCacheSizeCommand)resultCommand).CacheSize;
                    break;

                case TCommandCode.EXCEPTION:
                    throw new Exception(((TExceptionCommand)resultCommand).Exception);

                default:
                    break;
            }
        }

        #endregion

        #region IDisposable Members

        private volatile bool disposed = false;

        private void Dispose(bool disposing)
        {
            if (!disposed)
            {
                if (disposing)
                {
                    ClientConnection.Stop();
                }

                disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        ~TStorageEngineClient()
        {
            Dispose(false);
        }

        public void Close()
        {
            Dispose();
        }

        #endregion

        public int CacheSize
        {
            get
            {
                TStorageEngineGetCacheSizeCommand command = new TStorageEngineGetCacheSizeCommand(0);

                TCommandCollection collection = new TCommandCollection(1);
                collection.Add(command);

                TStorageEngineGetCacheSizeCommand resultComamnd = (TStorageEngineGetCacheSizeCommand)Execute(StorageEngineDescriptor, collection)[0];

                return resultComamnd.CacheSize;
            }
            set
            {
                cacheSize = value;
                TStorageEngineSetCacheSizeCommand command = new TStorageEngineSetCacheSizeCommand(cacheSize);

                TCommandCollection collection = new TCommandCollection(1);
                collection.Add(command);

                Execute(StorageEngineDescriptor, collection);
            }
        }

        private class RemoteHeap : ITHeap
        {
            public TStorageEngineClient Engine { get; private set; }

            public RemoteHeap(TStorageEngineClient engine)
            {
                if (engine == null)
                    throw new ArgumentNullException("engine");

                Engine = engine;
            }

            private void InternalExecute(ITCommand command)
            {
                TCommandCollection cmds = new TCommandCollection(1);
                cmds.Add(command);

                var resultCommand = Engine.Execute(StorageEngineDescriptor, cmds)[0];
                SetResult(command, resultCommand);
            }

            #region ITHeap

            public long ObtainNewHandle()
            {
                var cmd = new THeapObtainNewHandleCommand();
                InternalExecute(cmd);

                return cmd.Handle;
            }

            public void Release(long handle)
            {
                InternalExecute(new THeapReleaseHandleCommand(handle));
            }

            public bool Exists(long handle)
            {
                var cmd = new THeapExistsHandleCommand(handle, false);
                InternalExecute(cmd);

                return cmd.Exist;
            }

            public void Write(long handle, byte[] buffer, int index, int count)
            {
                InternalExecute(new THeapWriteCommand(handle, buffer, index, count));
            }

            public byte[] Read(long handle)
            {
                var cmd = new THeapReadCommand(handle, null);
                InternalExecute(cmd);

                return cmd.Buffer;
            }

            public void Commit()
            {
                InternalExecute(new THeapCommitCommand());
            }

            public void Close()
            {
                InternalExecute(new THeapCloseCommand());
            }

            public byte[] Tag
            {
                get
                {
                    var cmd = new THeapGetTagCommand();
                    InternalExecute(cmd);

                    return cmd.Tag;
                }
                set
                {
                    InternalExecute(new THeapSetTagCommand(value));
                }
            }

            public long DataSize
            {
                get
                {
                    var cmd = new THeapDataSizeCommand();
                    InternalExecute(cmd);

                    return cmd.DataSize;
                }
            }

            public long Size
            {
                get
                {
                    var cmd = new THeapSizeCommand();
                    InternalExecute(cmd);

                    return cmd.Size;
                }
            }

            #endregion

            private void SetResult(ITCommand command, ITCommand resultCommand)
            {
                switch (resultCommand.Code)
                {
                    case TCommandCode.HEAP_OBTAIN_NEW_HANDLE:
                        ((THeapObtainNewHandleCommand)command).Handle = ((THeapObtainNewHandleCommand)resultCommand).Handle;
                        break;

                    case TCommandCode.HEAP_RELEASE_HANDLE:
                        break;

                    case TCommandCode.HEAP_EXISTS_HANDLE:
                        ((THeapExistsHandleCommand)command).Exist = ((THeapExistsHandleCommand)resultCommand).Exist;
                        break;

                    case TCommandCode.HEAP_WRITE:
                        break;

                    case TCommandCode.HEAP_READ:
                        ((THeapReadCommand)command).Buffer = ((THeapReadCommand)resultCommand).Buffer;
                        break;

                    case TCommandCode.HEAP_COMMIT:
                        break;

                    case TCommandCode.HEAP_CLOSE:
                        break;

                    case TCommandCode.HEAP_SET_TAG:
                        break;

                    case TCommandCode.HEAP_GET_TAG:
                        ((THeapGetTagCommand)command).Tag = ((THeapGetTagCommand)resultCommand).Tag;
                        break;

                    case TCommandCode.HEAP_DATA_SIZE:
                        ((THeapDataSizeCommand)command).DataSize = ((THeapDataSizeCommand)resultCommand).DataSize;
                        break;

                    case TCommandCode.HEAP_SIZE:
                        ((THeapSizeCommand)command).Size = ((THeapSizeCommand)resultCommand).Size;
                        break;

                    case TCommandCode.EXCEPTION:
                        throw new Exception(((TExceptionCommand)resultCommand).Exception);

                    default:
                        break;
                }
            }
        }
    }

}
