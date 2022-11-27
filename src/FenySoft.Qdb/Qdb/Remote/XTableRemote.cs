using System.Collections;

using FenySoft.Core.Data;
using FenySoft.Qdb.Database;
using FenySoft.Qdb.Remote.Commands;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote
{
    public class XTableRemote : ITTable<ITData, ITData>
    {
        private int PageCapacity = 100000;
        private CommandCollection Commands;

        public Descriptor IndexDescriptor;
        public readonly StorageEngineClient StorageEngine;

        internal XTableRemote(StorageEngineClient storageEngine, Descriptor descriptor)
        {
            StorageEngine = storageEngine;
            IndexDescriptor = descriptor;

            Commands = new CommandCollection(100 * 1024);
        }

        ~XTableRemote()
        {
            Flush();
        }

        private void InternalExecute(ICommand command)
        {
            if (Commands.Capacity == 0)
            {
                CommandCollection commands = new CommandCollection(1);
                commands.Add(command);

                var resultCommands = StorageEngine.Execute(IndexDescriptor, commands);
                SetResult(commands, resultCommands);

                return;
            }

            Commands.Add(command);
            if (Commands.Count == Commands.Capacity || command.IsSynchronous)
                Flush();
        }

        public void Execute(ICommand command)
        {
            InternalExecute(command);
        }

        public void Execute(CommandCollection commands)
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
            Execute(new ReplaceCommand(key, record));
        }

        public void InsertOrIgnore(ITData key, ITData record)
        {
            Execute(new InsertOrIgnoreCommand(key, record));
        }

        public void Delete(ITData key)
        {
            Execute(new DeleteCommand(key));
        }

        public void Delete(ITData fromKey, ITData toKey)
        {
            Execute(new DeleteRangeCommand(fromKey, toKey));
        }

        public void Clear()
        {
            Execute(new ClearCommand());
        }

        public bool Exists(ITData key)
        {
            ITData record;

            return TryGet(key, out record);
        }

        public bool TryGet(ITData key, out ITData record)
        {
            var command = new TryGetCommand(key);
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
            var command = new FindNextCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindAfter(ITData key)
        {
            var command = new FindAfterCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindPrev(ITData key)
        {
            var command = new FindPrevCommand(key);
            Execute(command);

            return command.KeyValue;
        }

        public KeyValuePair<ITData, ITData>? FindBefore(ITData key)
        {
            var command = new FindBeforeCommand(key);
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

            var command = new ForwardCommand(PageCapacity, from, to, null);
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
                        var _command = new ForwardCommand(PageCapacity, nextKey, to, null);
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

            var command = new BackwardCommand(PageCapacity, to, from, null);
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
                        var _command = new BackwardCommand(PageCapacity, nextKey, from, null);
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
                var command = new FirstRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public KeyValuePair<ITData, ITData> LastRow
        {
            get
            {
                var command = new LastRowCommand();
                Execute(command);

                return command.Row.Value;
            }
        }

        public long Count()
        {
            var command = new CountCommand();
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

        private void SetResult(CommandCollection commands, CommandCollection resultCommands)
        {
            var command = commands[commands.Count - 1];
            if (!command.IsSynchronous)
                return;

            var resultOperation = resultCommands[resultCommands.Count - 1];

            try
            {
                switch (command.Code)
                {
                    case CommandCode.TRY_GET:
                        ((TryGetCommand)command).Record = ((TryGetCommand)resultOperation).Record;
                        break;
                    case CommandCode.FORWARD:
                        ((ForwardCommand)command).List = ((ForwardCommand)resultOperation).List;
                        break;
                    case CommandCode.BACKWARD:
                        ((BackwardCommand)command).List = ((BackwardCommand)resultOperation).List;
                        break;
                    case CommandCode.FIND_NEXT:
                        ((FindNextCommand)command).KeyValue = ((FindNextCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_AFTER:
                        ((FindAfterCommand)command).KeyValue = ((FindAfterCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_PREV:
                        ((FindPrevCommand)command).KeyValue = ((FindPrevCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIND_BEFORE:
                        ((FindBeforeCommand)command).KeyValue = ((FindBeforeCommand)resultOperation).KeyValue;
                        break;
                    case CommandCode.FIRST_ROW:
                        ((FirstRowCommand)command).Row = ((FirstRowCommand)resultOperation).Row;
                        break;
                    case CommandCode.LAST_ROW:
                        ((LastRowCommand)command).Row = ((LastRowCommand)resultOperation).Row;
                        break;
                    case CommandCode.COUNT:
                        ((CountCommand)command).Count = ((CountCommand)resultOperation).Count;
                        break;
                    case CommandCode.STORAGE_ENGINE_COMMIT:
                        break;
                    case CommandCode.EXCEPTION:
                        throw new Exception(((ExceptionCommand)command).Exception);
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
            set { IndexDescriptor = (Descriptor)value; }
        }

        private void GetDescriptor()
        {
            XTableDescriptorGetCommand command = new XTableDescriptorGetCommand(this.Descriptor);

            CommandCollection collection = new CommandCollection(1);
            collection.Add(command);

            collection = StorageEngine.Execute(this.Descriptor, collection);
            XTableDescriptorGetCommand resultCommand = (XTableDescriptorGetCommand)collection[0];

            this.Descriptor = resultCommand.Descriptor;
        }

        private void SetDescriptor()
        {
            XTableDescriptorSetCommand command = new XTableDescriptorSetCommand(this.Descriptor);

            CommandCollection collection = new CommandCollection(1);
            collection.Add(command);

            collection = StorageEngine.Execute(this.Descriptor, collection);
            XTableDescriptorSetCommand resultCommand = (XTableDescriptorSetCommand)collection[0]; 
        }

        /// <summary>
        /// Updates the local descriptor with the changes from the remote
        /// and retrieves up to date descriptor from the local server.
        /// </summary>
        private void UpdateDescriptor()
        {
            ICommand command = null;
            CommandCollection collection = new CommandCollection(1);

            // Set the local descriptor
            command = new XTableDescriptorSetCommand(this.Descriptor);
            collection.Add(command);

            StorageEngine.Execute(this.Descriptor, collection);

            // Get the local descriptor
            command = new XTableDescriptorGetCommand(this.Descriptor);
            collection.Clear();

            collection.Add(command);
            collection = StorageEngine.Execute(this.Descriptor, collection);

            XTableDescriptorGetCommand resultCommand = (XTableDescriptorGetCommand)collection[0];
            this.Descriptor = resultCommand.Descriptor;
        }
    }
}