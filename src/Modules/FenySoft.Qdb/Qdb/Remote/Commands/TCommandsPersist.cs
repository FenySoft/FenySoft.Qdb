using FenySoft.Core.Data;
using FenySoft.Core.Compression;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote.Commands
{
    public partial class TCommandPersist
    {
        #region XIndex Commands

        private void WriteReplaceCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TReplaceCommand)command;

            KeyPersist.Write(writer, cmd.Key);
            RecordPersist.Write(writer, cmd.Record);
        }

        private TReplaceCommand ReadReplaceCommand(BinaryReader reader)
        {
            return new TReplaceCommand(KeyPersist.Read(reader), RecordPersist.Read(reader));
        }

        private void WriteDeleteCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TDeleteCommand)command;

            KeyPersist.Write(writer, cmd.Key);
        }

        private TDeleteCommand ReadDeleteCommand(BinaryReader reader)
        {
            return new TDeleteCommand(KeyPersist.Read(reader));
        }

        private void WriteDeleteRangeCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TDeleteRangeCommand)command;

            KeyPersist.Write(writer, cmd.FromKey);
            KeyPersist.Write(writer, cmd.ToKey);
        }

        private TDeleteRangeCommand ReadDeleteRangeCommand(BinaryReader reader)
        {
            return new TDeleteRangeCommand(KeyPersist.Read(reader), KeyPersist.Read(reader));
        }

        private void WriteClearCommand(BinaryWriter writer, ITCommand command)
        {
        }

        private TClearCommand ReadClearCommand(BinaryReader reader)
        {
            return new TClearCommand();
        }

        private void WriteInsertOrIgnoreCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TInsertOrIgnoreCommand)command;

            KeyPersist.Write(writer, cmd.Key);
            RecordPersist.Write(writer, cmd.Record);
        }

        private TInsertOrIgnoreCommand ReadInsertOrIgnoreCommand(BinaryReader reader)
        {
            return new TInsertOrIgnoreCommand(KeyPersist.Read(reader), RecordPersist.Read(reader));
        }

        private void WriteTryGetCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TTryGetCommand)command;

            KeyPersist.Write(writer, cmd.Key);

            writer.Write(cmd.Record != null);
            if (cmd.Record != null)
                RecordPersist.Write(writer, cmd.Record);
        }

        private TTryGetCommand ReadTryGetCommand(BinaryReader reader)
        {
            return new TTryGetCommand(KeyPersist.Read(reader), reader.ReadBoolean() ? RecordPersist.Read(reader) : null);
        }

        private void WriteForwardCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TForwardCommand)command;

            writer.Write(cmd.PageCount);

            writer.Write(cmd.FromKey != null);
            if (cmd.FromKey != null)
                KeyPersist.Write(writer, cmd.FromKey);

            writer.Write(cmd.ToKey != null);
            if (cmd.ToKey != null)
                KeyPersist.Write(writer, cmd.ToKey);

            writer.Write(cmd.List != null);
            if (cmd.List != null)
                SerializeList(writer, cmd.List, cmd.List.Count);
        }

        private TForwardCommand ReadForwardCommand(BinaryReader reader)
        {
            int pageCount = reader.ReadInt32();
            ITData from = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
            ITData to = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
            List<KeyValuePair<ITData, ITData>> list = reader.ReadBoolean() ? DeserializeList(reader) : null;

            return new TForwardCommand(pageCount, from, to, list);
        }

        private void WriteBackwardCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TBackwardCommand)command;

            writer.Write(cmd.PageCount);

            writer.Write(cmd.FromKey != null);
            if (cmd.FromKey != null)
                KeyPersist.Write(writer, cmd.FromKey);

            writer.Write(cmd.ToKey != null);
            if (cmd.ToKey != null)
                KeyPersist.Write(writer, cmd.ToKey);

            writer.Write(cmd.List != null);
            if (cmd.List != null)
                SerializeList(writer, cmd.List, cmd.List.Count);
        }

        private TBackwardCommand ReadBackwardCommand(BinaryReader reader)
        {
            int pageCount = reader.ReadInt32();
            ITData from = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
            ITData to = reader.ReadBoolean() ? KeyPersist.Read(reader) : null;
            List<KeyValuePair<ITData, ITData>> list = reader.ReadBoolean() ? DeserializeList(reader) : null;

            return new TBackwardCommand(pageCount, from, to, list);
        }

        private void WriteFindNextCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TFindNextCommand)command;

            KeyPersist.Write(writer, cmd.Key);

            writer.Write(cmd.KeyValue.HasValue);
            if (cmd.KeyValue.HasValue)
            {
                KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
                RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
            }
        }

        private TFindNextCommand ReadFindNextCommand(BinaryReader reader)
        {
            ITData Key = KeyPersist.Read(reader);

            bool hasValue = reader.ReadBoolean();
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TFindNextCommand(Key, hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteFindAfterCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TFindAfterCommand)command;

            KeyPersist.Write(writer, cmd.Key);

            writer.Write(cmd.KeyValue.HasValue);
            if (cmd.KeyValue.HasValue)
            {
                KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
                RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
            }
        }

        private TFindAfterCommand ReadFindAfterCommand(BinaryReader reader)
        {
            ITData Key = KeyPersist.Read(reader);

            bool hasValue = (reader.ReadBoolean());
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TFindAfterCommand(Key, hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteFindPrevCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TFindPrevCommand)command;

            KeyPersist.Write(writer, cmd.Key);

            writer.Write(cmd.KeyValue.HasValue);
            if (cmd.KeyValue.HasValue)
            {
                KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
                RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
            }
        }

        private TFindPrevCommand ReadFindPrevCommand(BinaryReader reader)
        {
            ITData Key = KeyPersist.Read(reader);

            bool hasValue = (reader.ReadBoolean());
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TFindPrevCommand(Key, hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteFindBeforeCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TFindBeforeCommand)command;

            KeyPersist.Write(writer, cmd.Key);

            writer.Write(cmd.KeyValue.HasValue);
            if (cmd.KeyValue.HasValue)
            {
                KeyPersist.Write(writer, cmd.KeyValue.Value.Key);
                RecordPersist.Write(writer, cmd.KeyValue.Value.Value);
            }
        }

        private TFindBeforeCommand ReadFindBeforeCommand(BinaryReader reader)
        {
            ITData Key = KeyPersist.Read(reader);

            bool hasValue = (reader.ReadBoolean());
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TFindBeforeCommand(Key, hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteFirstRowCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TFirstRowCommand)command;

            writer.Write(cmd.Row.HasValue);
            if (cmd.Row.HasValue)
            {
                KeyPersist.Write(writer, cmd.Row.Value.Key);
                RecordPersist.Write(writer, cmd.Row.Value.Value);
            }
        }

        private TFirstRowCommand ReadFirstRowCommand(BinaryReader reader)
        {
            bool hasValue = (reader.ReadBoolean());
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TFirstRowCommand(hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteLastRowCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TLastRowCommand)command;

            writer.Write(cmd.Row.HasValue);
            if (cmd.Row.HasValue)
            {
                KeyPersist.Write(writer, cmd.Row.Value.Key);
                RecordPersist.Write(writer, cmd.Row.Value.Value);
            }
        }

        private TLastRowCommand ReadLastRowCommand(BinaryReader reader)
        {
            bool hasValue = (reader.ReadBoolean());
            ITData key = hasValue ? KeyPersist.Read(reader) : null;
            ITData rec = hasValue ? RecordPersist.Read(reader) : null;

            return new TLastRowCommand(hasValue ? (KeyValuePair<ITData, ITData>?)new KeyValuePair<ITData, ITData>(key, rec) : null);
        }

        private void WriteCountCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TCountCommand)command;
            writer.Write(cmd.Count);
        }

        private TCountCommand ReadCountCommand(BinaryReader reader)
        {
            return new TCountCommand(reader.ReadInt64());
        }

        private void WriteXIndexDescriptorGetCommand(BinaryWriter writer, ITCommand command)
        {
            TXTableDescriptorGetCommand cmd = (TXTableDescriptorGetCommand)command;
            ITDescriptor descriptor = cmd.Descriptor;

            writer.Write(descriptor != null);

            if (descriptor != null)
                SerializeDescriptor(writer, descriptor);
        }

        private TXTableDescriptorGetCommand ReadXIndexDescriptorGetCommand(BinaryReader reader)
        {
            ITDescriptor description = null;

            if (reader.ReadBoolean()) // Description != null
                description = TDescriptor.Deserialize(reader);

            return new TXTableDescriptorGetCommand(description);
        }

        private void WriteXIndexDescriptorSetCommand(BinaryWriter writer, ITCommand command)
        {
            TXTableDescriptorSetCommand cmd = (TXTableDescriptorSetCommand)command;
            TDescriptor descriptor = (TDescriptor)cmd.Descriptor;

            writer.Write(descriptor != null);

            if (descriptor != null)
                descriptor.Serialize(writer);
        }

        private TXTableDescriptorSetCommand ReadXIndexDescriptorSetCommand(BinaryReader reader)
        {
            ITDescriptor descriptor = null;

            if (reader.ReadBoolean()) // TDescriptor != null
                descriptor = TDescriptor.Deserialize(reader);

            return new TXTableDescriptorSetCommand(descriptor);
        }

        #endregion

        #region Storage EngineCommands

        private void WriteStorageEngineCommitCommand(BinaryWriter writer, ITCommand command)
        {
        }

        private TStorageEngineCommitCommand ReadStorageEngineCommitCommand(BinaryReader reader)
        {
            return new TStorageEngineCommitCommand();
        }

        private void WriteStorageEngineGetEnumeratorCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineGetEnumeratorCommand)command;

            if (cmd.Descriptions == null)
                writer.Write(true);
            else
            {
                writer.Write(false);

                int listCount = cmd.Descriptions.Count;
                TCountCompression.Serialize(writer, (ulong)listCount);

                for (int i = 0; i < listCount; i++)
                    SerializeDescriptor(writer, cmd.Descriptions[i]);
            }
        }

        private TStorageEngineGetEnumeratorCommand ReadStorageEngineGetEnumeratorCommand(BinaryReader reader)
        {
            bool isListNull = reader.ReadBoolean();
            List<ITDescriptor> descriptions = new List<ITDescriptor>();

            if (!isListNull)
            {
                int listCount = (int)TCountCompression.Deserialize(reader);

                for (int i = 0; i < listCount; i++)
                    descriptions.Add((TDescriptor)DeserializeDescriptor(reader));
            }

            return new TStorageEngineGetEnumeratorCommand(descriptions);
        }

        private void WriteStorageEngineRenameCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineRenameCommand)command;

            writer.Write(cmd.Name);
            writer.Write(cmd.NewName);
        }

        private TStorageEngineRenameCommand ReadStorageEngineRenameCommand(BinaryReader reader)
        {
            string name = reader.ReadString();
            string newName = reader.ReadString();

            return new TStorageEngineRenameCommand(name, newName);
        }

        private void WriteStorageEngineExistCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineExistsCommand)command;

            writer.Write(cmd.Name);
            writer.Write(cmd.Exist);
        }

        private TStorageEngineExistsCommand ReadStorageEngineExistCommand(BinaryReader reader)
        {
            string name = reader.ReadString();
            bool exist = reader.ReadBoolean();

            return new TStorageEngineExistsCommand(exist, name);
        }

        private void WriteStorageEngineFindByIDCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineFindByIDCommand)command;

            writer.Write(cmd.ID);

            writer.Write(cmd.Descriptor != null);
            if (cmd.Descriptor != null)
                SerializeDescriptor(writer, cmd.Descriptor);
        }

        private TStorageEngineFindByIDCommand ReadStorageEngineFindByIDCommand(BinaryReader reader)
        {
            long id = reader.ReadInt64();
            var schemeRecord = reader.ReadBoolean() ? DeserializeDescriptor(reader) : null;

            return new TStorageEngineFindByIDCommand(schemeRecord, id);
        }

        private void WriteStorageEngineOpenXIndexCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineOpenXIndexCommand)command;

            writer.Write(cmd.ID);
            if (cmd.ID < 0)
            {
                cmd.KeyType.Serialize(writer);
                cmd.RecordType.Serialize(writer);

                writer.Write(cmd.Name);
            }
        }

        private TStorageEngineOpenXIndexCommand ReadStorageEngineOpenXIndexCommand(BinaryReader reader)
        {
            long id = reader.ReadInt64();

            if (id < 0)
            {
                var keyType = TDataType.Deserialize(reader);
                var recordType = TDataType.Deserialize(reader);

                string name = reader.ReadString();

                return new TStorageEngineOpenXIndexCommand(name, keyType, recordType);
            }

            return new TStorageEngineOpenXIndexCommand(id);
        }

        private void WriteStorageEngineOpenXFileCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineOpenXFileCommand)command;

            writer.Write(cmd.Name == null);
            if (cmd.Name == null)
                writer.Write(cmd.ID);
            else
                writer.Write(cmd.Name);
        }

        private TStorageEngineOpenXFileCommand ReadStorageEngineOpenXFileCommand(BinaryReader reader)
        {
            if (reader.ReadBoolean())
                return new TStorageEngineOpenXFileCommand(reader.ReadInt64());
            else
                return new TStorageEngineOpenXFileCommand(reader.ReadString());
        }

        private void WriteStorageEngineDeleteCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineDeleteCommand)command;
            writer.Write(cmd.Name);
        }

        private TStorageEngineDeleteCommand ReadStorageEngineDeleteCommand(BinaryReader reader)
        {
            return new TStorageEngineDeleteCommand(reader.ReadString());
        }

        private void WriteStorageEngineCountCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineCountCommand)command;
            TCountCompression.Serialize(writer, (ulong)cmd.Count);
        }

        private TStorageEngineCountCommand ReadStorageEngineCountCommand(BinaryReader reader)
        {
            return new TStorageEngineCountCommand((int)TCountCompression.Deserialize(reader));
        }

        private void WriteStorageEngineFindByNameCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TStorageEngineFindByNameCommand)command;

            writer.Write(cmd.Name);
            writer.Write(cmd.Descriptor != null);

            if (cmd.Descriptor != null)
                SerializeDescriptor(writer, cmd.Descriptor);
        }

        private TStorageEngineFindByNameCommand ReadStorageEngineFindByNameCommand(BinaryReader reader)
        {
            string name = reader.ReadString();
            var description = reader.ReadBoolean() ? DeserializeDescriptor(reader) : null;

            return new TStorageEngineFindByNameCommand(name, description);
        }

        private void WriteStorageEngineDescriptionCommand(BinaryWriter writer, ITCommand command)
        {
            TStorageEngineDescriptionCommand cmd = (TStorageEngineDescriptionCommand)command;
            ITDescriptor description = cmd.Descriptor;

            writer.Write(description != null);

            if (description != null)
                SerializeDescriptor(writer, description);
        }

        private TStorageEngineDescriptionCommand ReadStorageEngineDescriptionCommand(BinaryReader reader)
        {
            ITDescriptor description = null;

            if (reader.ReadBoolean()) // Description != null
                description = DeserializeDescriptor(reader);

            return new TStorageEngineDescriptionCommand(description);
        }

        private void WriteStorageEngineGetCacheCommand(BinaryWriter writer, ITCommand command)
        {
            TStorageEngineGetCacheSizeCommand cmd = (TStorageEngineGetCacheSizeCommand)command;

            writer.Write(cmd.CacheSize);
        }

        private TStorageEngineGetCacheSizeCommand ReadStorageEngineGetCacheSizeCommand(BinaryReader reader)
        {
            int cacheSize = reader.ReadInt32();

            return new TStorageEngineGetCacheSizeCommand(cacheSize);
        }

        private void WriteStorageEngineSetCacheCommand(BinaryWriter writer, ITCommand command)
        {
            TStorageEngineSetCacheSizeCommand cmd = (TStorageEngineSetCacheSizeCommand)command;

            writer.Write(cmd.CacheSize);
        }

        private TStorageEngineSetCacheSizeCommand ReadStorageEngineSetCacheCommand(BinaryReader reader)
        {
            int cacheSize = reader.ReadInt32();

            return new TStorageEngineSetCacheSizeCommand(cacheSize);
        }

        #endregion

        #region HeapCommands

        private void WriteHeapObtainNewHandleCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapObtainNewHandleCommand)command;
            writer.Write(cmd.Handle);
        }

        private THeapObtainNewHandleCommand ReadHeapObtainNewHandleCommand(BinaryReader reader)
        {
            return new THeapObtainNewHandleCommand(reader.ReadInt64());
        }

        private void WriteHeapReleaseHandleCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapReleaseHandleCommand)command;
            writer.Write(cmd.Handle);
        }

        private THeapReleaseHandleCommand ReadHeapReleaseHandleCommand(BinaryReader reader)
        {
            return new THeapReleaseHandleCommand(reader.ReadInt64());
        }

        private void WriteHeapExistsHandleCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapExistsHandleCommand)command;
            writer.Write(cmd.Handle);
            writer.Write(cmd.Exist);
        }

        private THeapExistsHandleCommand ReadHeapExistsHandleCommand(BinaryReader reader)
        {
            return new THeapExistsHandleCommand(reader.ReadInt64(), reader.ReadBoolean());
        }

        private void WriteHeapWriteCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapWriteCommand)command;

            writer.Write(cmd.Handle);

            writer.Write(cmd.Count);
            writer.Write(cmd.Index);

            if (cmd.Buffer == null)
                writer.Write(false);
            else
            {
                writer.Write(true);
                writer.Write(cmd.Buffer.Length);
                writer.Write(cmd.Buffer, 0, cmd.Buffer.Length);
            }
        }

        private THeapWriteCommand ReadHeapWriteCommand(BinaryReader reader)
        {
            var handle = reader.ReadInt64();

            var count = reader.ReadInt32();
            var index = reader.ReadInt32();

            byte[] buffer = null; ;
            if (reader.ReadBoolean())
            {
                buffer = new byte[reader.ReadInt32()];
                reader.Read(buffer, 0, buffer.Length);
            }

            return new THeapWriteCommand(handle, buffer, index, count);
        }

        private void WriteHeapReadCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapReadCommand)command;

            writer.Write(cmd.Handle);

            if (cmd.Buffer == null)
                writer.Write(false);
            else
            {
                writer.Write(true);
                writer.Write(cmd.Buffer.Length);
                writer.Write(cmd.Buffer);
            }
        }

        private THeapReadCommand ReadHeapReadCommand(BinaryReader reader)
        {
            var handle = reader.ReadInt64();

            byte[] buffer = null;
            if (reader.ReadBoolean())
            {
                int count = reader.ReadInt32();
                buffer = reader.ReadBytes(count);
            }

            return new THeapReadCommand(handle, buffer);
        }

        private void WriteHeapCommitCommand(BinaryWriter writer, ITCommand command)
        {
        }

        private THeapCommitCommand ReadHeapCommitCommand(BinaryReader reader)
        {
            return new THeapCommitCommand();
        }

        private void WriteHeapCloseCommand(BinaryWriter writer, ITCommand command)
        {
        }

        private THeapCloseCommand ReadHeapCloseCommand(BinaryReader reader)
        {
            return new THeapCloseCommand();
        }

        public void WriteHeapSetTagCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapSetTagCommand)command;

            if (cmd.Buffer == null)
                writer.Write(false);
            else
            {
                writer.Write(true);
                writer.Write(cmd.Buffer.Length);
                writer.Write(cmd.Buffer);
            }
        }

        public THeapSetTagCommand ReadHeapSetTagCommand(BinaryReader reader)
        {
            byte[] buffer = null;
            if (reader.ReadBoolean())
            {
                int count = reader.ReadInt32();
                buffer = new byte[count];

                reader.Read(buffer, 0, count);
            }

            return new THeapSetTagCommand(buffer);
        }

        public void WriteHeapGetTagCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapGetTagCommand)command;

            if (cmd.Tag == null)
                writer.Write(false);
            else
            {
                writer.Write(true);
                writer.Write(cmd.Tag.Length);
                writer.Write(cmd.Tag);
            }
        }

        public THeapGetTagCommand ReadHeapGetTagCommand(BinaryReader reader)
        {
            byte[] tag = null;
            if (reader.ReadBoolean())
            {
                int count = reader.ReadInt32();
                tag = new byte[count];

                reader.Read(tag, 0, count);
            }

            return new THeapGetTagCommand(tag);
        }

        public void WriteHeapDataSizeCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapDataSizeCommand)command;

            writer.Write(cmd.DataSize);
        }

        public THeapDataSizeCommand ReadHeapDataSizeCommand(BinaryReader reader)
        {
            return new THeapDataSizeCommand(reader.ReadInt64());
        }

        public void WriteHeapSizeCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (THeapSizeCommand)command;

            writer.Write(cmd.Size);
        }

        public THeapSizeCommand ReadHeapSizeCommand(BinaryReader reader)
        {
            return new THeapSizeCommand(reader.ReadInt64());
        }

        #endregion

        #region Other Commands

        private void WriteExceptionCommand(BinaryWriter writer, ITCommand command)
        {
            var cmd = (TExceptionCommand)command;
            writer.Write(cmd.Exception);
        }

        private TExceptionCommand ReadExceptionCommand(BinaryReader reader)
        {
            return new TExceptionCommand(reader.ReadString());
        }

        #endregion

        #region Helper Methods

        private void SerializeList(BinaryWriter writer, List<KeyValuePair<ITData, ITData>> list, int count)
        {
            writer.Write(count);

            foreach (var kv in list)
            {
                KeyPersist.Write(writer, kv.Key);
                RecordPersist.Write(writer, kv.Value);
            }
        }

        private List<KeyValuePair<ITData, ITData>> DeserializeList(BinaryReader reader)
        {
            int count = reader.ReadInt32();

            List<KeyValuePair<ITData, ITData>> list = new List<KeyValuePair<ITData, ITData>>(count);
            for (int i = 0; i < count; i++)
            {
                ITData key = KeyPersist.Read(reader);
                ITData rec = RecordPersist.Read(reader);

                list.Add(new KeyValuePair<ITData, ITData>(key, rec));
            }

            return list;
        }

        private void SerializeDescriptor(BinaryWriter writer, ITDescriptor description)
        {
            TCountCompression.Serialize(writer, (ulong)description.ID);
            writer.Write(description.Name);

            TCountCompression.Serialize(writer, (ulong)description.StructureType);

            description.KeyDataType.Serialize(writer);
            description.RecordDataType.Serialize(writer);

            TCountCompression.Serialize(writer, (ulong)description.CreateTime.Ticks);
            TCountCompression.Serialize(writer, (ulong)description.ModifiedTime.Ticks);
            TCountCompression.Serialize(writer, (ulong)description.AccessTime.Ticks);

            if (description.Tag == null)
                TCountCompression.Serialize(writer, 0);
            else
            {
                TCountCompression.Serialize(writer, (ulong)description.Tag.Length + 1);
                writer.Write(description.Tag);
            }
        }

        private ITDescriptor DeserializeDescriptor(BinaryReader reader)
        {
            long id = (long)TCountCompression.Deserialize(reader);
            string name = reader.ReadString();

            int structureType = (int)TCountCompression.Deserialize(reader);

            var keyDataType = TDataType.Deserialize(reader);
            var recordDataType = TDataType.Deserialize(reader);

            var keyType = TDataTypeUtils.BuildType(keyDataType);
            var recordType = TDataTypeUtils.BuildType(recordDataType);

            var createTime = new DateTime((long)TCountCompression.Deserialize(reader));
            var modifiedTime = new DateTime((long)TCountCompression.Deserialize(reader));
            var accessTime = new DateTime((long)TCountCompression.Deserialize(reader));

            var tagLength = (int)TCountCompression.Deserialize(reader) - 1;
            byte[] tag = tagLength >= 0 ? reader.ReadBytes(tagLength) : null;

            return new TDescriptor(id, name, structureType, keyDataType, recordDataType, keyType, recordType, createTime, modifiedTime, accessTime, tag);
        }

        #endregion
    }
}

