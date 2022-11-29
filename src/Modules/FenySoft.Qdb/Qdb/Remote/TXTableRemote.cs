using System.Collections;

using FenySoft.Core.Data;
using FenySoft.Qdb.Database;
using FenySoft.Qdb.Remote.Commands;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote
{
    public class TXTableRemote : ITTable<ITData, ITData>
    {
        private int PageCapacity = 100000;
        private TCommandCollection Commands;

        public TDescriptor IndexDescriptor;
        public readonly TStorageEngineClient StorageEngine;

        internal TXTableRemote(TStorageEngineClient storageEngine, TDescriptor descriptor)
        {
            StorageEngine = storageEngine;
            IndexDescriptor = descriptor;

            Commands = new TCommandCollection(100 * 1024);
        }

        ~TXTableRemote()
        {
            Flush();
        }

        private void InternalExecute(ITCommand command)
        {
            if (Commands.Capacity == 0)
            {
                TCommandCollection commands = new TCommandCollection(1);
                commands.Add(command);

                var resultCommands = StorageEngine.Execute(IndexDescriptor, commands);
                SetResult(commands, resultCommands);

                return;
            }

            Commands.Add(command);
            if (Commands.Count == Commands.Capacity || command.IsSynchronous)
                Flush();
        }

        public void Execute(ITCommand command)
        {
            InternalExecute(command);
        }

        public void Execute(TCommandCollection commands)
        {
            for (int i = 0; i < commands.Count; i++)
                Execute(commands[i]);
        }

        public void Flush()
        {
            if (Commands.Count == 0)
            {
                UpdateDescriptor();
                return;
            }

            UpdateDescriptor();

            var result = StorageEngine.Execute(IndexDescriptor, Commands);
            SetResult(Commands, result);

            Commands.Clear();
        }

        #region IIndex<IKey, IRecord>

        public ITData this[ITData key]
        {
            get
            {
                ITData record;
                if (!TryGet(key, out record))
                    throw new KeyNotFoundException(key.ToString());

                return record;
            }
            set
            {
                Replace(key, value);
            }
        }

        public void Replace(ITData key, ITData record)
        {
            Execute(new TReplaceCommand(key, record));
        }

        public void InsertOrIgnore(ITData key, ITData record)
        {
            Execute(new TInsertOrIgnoreCommand(key, record));
        }

        public void Delete(ITData key)
        {
            Execute(new TDeleteCommand(key));
        }

        public void Delete(ITData fromKey, ITData toKey)
        {
            Execute(new TDeleteRangeCommand(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new TClearCommand());
        }

        public bool Exists(ITData key)
        {
            ITData record;

            return TryGet(key, out record);
        }

        public bool TryGet(ITData key, out ITData record)
        {
            var command = new TTryGetCommand(key);
            Execute(command);

            record = command.Record;

            return record != null;
        }

        public ITData Find(ITData key)
        {
            ITData record;
            TryGet(key, out record);

            return record;
        }

        public ITData TryGetOrDefault(ITData key, ITData defaultRecord)
        {
            ITData record;
            if (!TryGet(key, out record))
                return defaultRecord;

            return record;
        }

        public KeyValuePair<ITData, ITData>? FindNext(ITData key)
        {
            var command = new TFindNextCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindAfter(ITData key)
        {
            var command = new TFindAfterCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindPrev(ITData key)
        {
            var command = new TFindPrevCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindBefore(ITData key)
        {
            var command = new TFindBeforeCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Forward()
        {
            return Forward(default(ITData), false, default(ITData), false);
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Forward(ITData from, bool hasFrom, ITData to, bool hasTo)
        {
            if (hasFrom && hasTo && IndexDescriptor.KeyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            from = hasFrom ? from : default(ITData);
            to = hasTo ? to : default(ITData);

            List<KeyValuePair<ITData, ITData>> records = null;
            ITData nextKey = null;

            var command = new TForwardCommand(PageCapacity, from, to, null);
            Execute(command);

            records = command.List;
            nextKey = records != null && records.Count == PageCapacity ? records[records.Count - 1].Key : null;

            while (records != null)
            {
                Task task = null;
                List<KeyValuePair<ITData, ITData>> _records = null;

                int returnCount = nextKey != null ? records.Count - 1 : records.Count;

                if (nextKey != null)
                {
                    task = Task.Factory.StartNew(() =>
                    {
                        var _command = new TForwardCommand(PageCapacity, nextKey, to, null);
                        Execute(_command);

                        _records = _command.List;
                        nextKey = _records != null && _records.Count == PageCapacity ? _records[_records.Count - 1].Key : null;
                    });
                }

                for (int i = 0; i < returnCount; i++)
                    yield return records[i];

                records = null;

                if (task != null)
                    task.Wait();

                if (_records != null)
                    records = _records;
            }
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Backward()
        {
            return Backward(default(ITData), false, default(ITData), false);
        }

        public IEnumerable<KeyValuePair<ITData, ITData>> Backward(ITData to, bool hasTo, ITData from, bool hasFrom)
        {
            if (hasFrom && hasTo && IndexDescriptor.KeyComparer.Compare(from, to) > 0)
                throw new ArgumentException("from > to");

            from = hasFrom ? from : default(ITData);
            to = hasTo ? to : default(ITData);

            List<KeyValuePair<ITData, ITData>> records = null;
            ITData nextKey = null;

            var command = new TBackwardCommand(PageCapacity, to, from, null);
            Execute(command);

            records = command.List;
            nextKey = records != null && records.Count == PageCapacity ? records[records.Count - 1].Key : null;

            while (records != null)
            {
                Task task = null;
                List<KeyValuePair<ITData, ITData>> _records = null;

                int returnCount = nextKey != null ? records.Count - 1 : records.Count;

                if (nextKey != null)
                {
                    task = Task.Factory.StartNew(() =>
                    {
                        var _command = new TBackwardCommand(PageCapacity, nextKey, from, null);
                        Execute(_command);

                        _records = _command.List;
                        nextKey = _records != null && _records.Count == PageCapacity ? _records[_records.Count - 1].Key : null;
                    });
                }

                for (int i = 0; i < returnCount; i++)
                    yield return records[i];

                records = null;

                if (task != null)
                    task.Wait();

                if (_records != null)
                    records = _records;
            }
        }

        public KeyValuePair<ITData, ITData> FirstRow
        {
            get
            {
                var command = new TFirstRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public KeyValuePair<ITData, ITData> LastRow
        {
            get
            {
                var command = new TLastRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public long Count()
        {
            var command = new TCountCommand();
            Execute(command);

            return command.Count;
        }

        public IEnumerator<KeyValuePair<ITData, ITData>> GetEnumerator()
        {
            return Forward().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        private void SetResult(TCommandCollection commands, TCommandCollection resultCommands)
        {
            var command = commands[commands.Count - 1];
            if (!command.IsSynchronous)
                return;

            var resultOperation = resultCommands[resultCommands.Count - 1];

            try
            {
                switch (command.Code)
                {
                    case TCommandCode.TRY_GET:
                        ((TTryGetCommand)command).Record = ((TTryGetCommand)resultOperation).Record;
                        break;
                    case TCommandCode.FORWARD:
                        ((TForwardCommand)command).List = ((TForwardCommand)resultOperation).List;
                        break;
                    case TCommandCode.BACKWARD:
                        ((TBackwardCommand)command).List = ((TBackwardCommand)resultOperation).List;
                        break;
                    case TCommandCode.FIND_NEXT:
                        ((TFindNextCommand)command).KeyValue = ((TFindNextCommand)resultOperation).KeyValue;
                        break;
                    case TCommandCode.FIND_AFTER:
                        ((TFindAfterCommand)command).KeyValue = ((TFindAfterCommand)resultOperation).KeyValue;
                        break;
                    case TCommandCode.FIND_PREV:
                        ((TFindPrevCommand)command).KeyValue = ((TFindPrevCommand)resultOperation).KeyValue;
                        break;
                    case TCommandCode.FIND_BEFORE:
                        ((TFindBeforeCommand)command).KeyValue = ((TFindBeforeCommand)resultOperation).KeyValue;
                        break;
                    case TCommandCode.FIRST_ROW:
                        ((TFirstRowCommand)command).Row = ((TFirstRowCommand)resultOperation).Row;
                        break;
                    case TCommandCode.LAST_ROW:
                        ((TLastRowCommand)command).Row = ((TLastRowCommand)resultOperation).Row;
                        break;
                    case TCommandCode.COUNT:
                        ((TCountCommand)command).Count = ((TCountCommand)resultOperation).Count;
                        break;
                    case TCommandCode.STORAGE_ENGINE_COMMIT:
                        break;
                    case TCommandCode.EXCEPTION:
                        throw new Exception(((TExceptionCommand)command).Exception);
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                throw new Exception(e.ToString());
            }
        }

        public ITDescriptor Descriptor
        {
            get { return IndexDescriptor; }
            set { IndexDescriptor = (TDescriptor)value; }
        }

        private void GetDescriptor()
        {
            TXTableDescriptorGetCommand command = new TXTableDescriptorGetCommand(Descriptor);

            TCommandCollection collection = new TCommandCollection(1);
            collection.Add(command);

            collection = StorageEngine.Execute(Descriptor, collection);
            TXTableDescriptorGetCommand resultCommand = (TXTableDescriptorGetCommand)collection[0];

            Descriptor = resultCommand.Descriptor;
        }

        private void SetDescriptor()
        {
            TXTableDescriptorSetCommand command = new TXTableDescriptorSetCommand(Descriptor);

            TCommandCollection collection = new TCommandCollection(1);
            collection.Add(command);

            collection = StorageEngine.Execute(Descriptor, collection);
            TXTableDescriptorSetCommand resultCommand = (TXTableDescriptorSetCommand)collection[0]; 
        }

        /// <summary>
        /// Updates the local descriptor with the changes from the remote
        /// and retrieves up to date descriptor from the local server.
        /// </summary>
        private void UpdateDescriptor()
        {
            ITCommand command = null;
            TCommandCollection collection = new TCommandCollection(1);

            // Set the local descriptor
            command = new TXTableDescriptorSetCommand(Descriptor);
            collection.Add(command);

            StorageEngine.Execute(Descriptor, collection);

            // Get the local descriptor
            command = new TXTableDescriptorGetCommand(Descriptor);
            collection.Clear();

            collection.Add(command);
            collection = StorageEngine.Execute(Descriptor, collection);

            TXTableDescriptorGetCommand resultCommand = (TXTableDescriptorGetCommand)collection[0];
            Descriptor = resultCommand.Descriptor;
        }
    }
}