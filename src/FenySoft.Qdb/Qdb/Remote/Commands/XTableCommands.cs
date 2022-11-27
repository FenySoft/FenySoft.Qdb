using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote.Commands
{
    #region ITTable Operations

    public class ReplaceCommand : ICommand
    {
        public ITData Key;
        public ITData Record;

        public ReplaceCommand(ITData key, ITData record)
        {
            Key = key;
            Record = record;
        }

        public int Code
        {
            get { return CommandCode.REPLACE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class DeleteCommand : ICommand
    {
        public ITData Key;

        public DeleteCommand(ITData key)
        {
            Key = key;
        }

        public int Code
        {
            get { return CommandCode.DELETE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class DeleteRangeCommand : ICommand
    {
        public ITData FromKey;
        public ITData ToKey;

        public DeleteRangeCommand(ITData fromKey, ITData toKey)
        {
            FromKey = fromKey;
            ToKey = toKey;
        }

        public int Code
        {
            get { return CommandCode.DELETE_RANGE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class InsertOrIgnoreCommand : ICommand
    {
        public ITData Key;
        public ITData Record;

        public InsertOrIgnoreCommand(ITData key, ITData record)
        {
            Key = key;
            Record = record;
        }

        public int Code
        {
            get { return CommandCode.INSERT_OR_IGNORE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class ClearCommand : ICommand
    {
        public ClearCommand()
        {
        }

        public int Code
        {
            get { return CommandCode.CLEAR; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class FirstRowCommand : ICommand
    {
        public KeyValuePair<ITData, ITData>? Row;

        public FirstRowCommand(KeyValuePair<ITData, ITData>? row)
        {
            Row = row;
        }

        public FirstRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.FIRST_ROW; }
        }
    }

    public class LastRowCommand : ICommand
    {
        public KeyValuePair<ITData, ITData>? Row;

        public LastRowCommand(KeyValuePair<ITData, ITData>? row)
        {
            Row = row;
        }

        public LastRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.LAST_ROW; }
        }
    }

    public class CountCommand : ICommand
    {
        public long Count;

        public CountCommand(long count)
        {
            Count = count;
        }

        public CountCommand()
            : this(0)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return CommandCode.COUNT; }
        }
    }

    public abstract class OutValueCommand : ICommand
    {
        private int code;

        public ITData Key;
        public ITData Record;

        public OutValueCommand(int code, ITData key, ITData record)
        {
            this.code = code;

            Key = key;
            Record = record;
        }

        public int Code
        {
            get { return code; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TryGetCommand : OutValueCommand
    {
        public TryGetCommand(ITData key, ITData record)
            : base(CommandCode.TRY_GET, key, record)
        {
        }

        public TryGetCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public abstract class OutKeyValueCommand : ICommand
    {
        private int code;

        public ITData Key;
        public KeyValuePair<ITData, ITData>? KeyValue;

        public OutKeyValueCommand(int code, ITData key, KeyValuePair<ITData, ITData>? keyValue)
        {
            this.code = code;

            Key = key;
            KeyValue = keyValue;
        }

        public int Code
        {
            get { return code; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class FindNextCommand : OutKeyValueCommand
    {
        public FindNextCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(CommandCode.FIND_NEXT, key, keyValue)
        {
        }

        public FindNextCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class FindAfterCommand : OutKeyValueCommand
    {
        public FindAfterCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(CommandCode.FIND_AFTER, key, keyValue)
        {
        }

        public FindAfterCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class FindPrevCommand : OutKeyValueCommand
    {
        public FindPrevCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(CommandCode.FIND_PREV, key, keyValue)
        {
        }

        public FindPrevCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class FindBeforeCommand : OutKeyValueCommand
    {
        public FindBeforeCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(CommandCode.FIND_BEFORE, key, keyValue)
        {
        }

        public FindBeforeCommand(ITData key)
            : this(key, null)
        {
        }
    }

    #endregion

    #region IteratorOperations

    public abstract class IteratorCommand : ICommand
    {
        private int code;

        public ITData FromKey;
        public ITData ToKey;

        public int PageCount;
        public List<KeyValuePair<ITData, ITData>> List;

        public IteratorCommand(int code, int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
        {
            this.code = code;

            FromKey = from;
            ToKey = to;

            PageCount = pageCount;
            List = list;
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return code; }
        }
    }

    public class ForwardCommand : IteratorCommand
    {
        public ForwardCommand(int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
            : base(CommandCode.FORWARD, pageCount, from, to, list)
        {
        }
    }

    public class BackwardCommand : IteratorCommand
    {
        public BackwardCommand(int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
            : base(CommandCode.BACKWARD, pageCount, from, to, list)
        {
        }
    }

    #endregion

    #region Descriptor

    public class XTableDescriptorGetCommand : ICommand
    {
        public ITDescriptor Descriptor;

        public XTableDescriptorGetCommand(ITDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code
        {
            get { return CommandCode.XTABLE_DESCRIPTOR_GET; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class XTableDescriptorSetCommand : ICommand
    {
        public ITDescriptor Descriptor;

        public XTableDescriptorSetCommand(ITDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code
        {
            get { return CommandCode.XTABLE_DESCRIPTOR_SET; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    #endregion
}
