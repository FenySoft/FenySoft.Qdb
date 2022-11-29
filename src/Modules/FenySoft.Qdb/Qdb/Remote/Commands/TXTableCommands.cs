using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Remote.Commands
{
    #region ITTable Operations

    public class TReplaceCommand : ITCommand
    {
        public ITData Key;
        public ITData Record;

        public TReplaceCommand(ITData key, ITData record)
        {
            Key = key;
            Record = record;
        }

        public int Code
        {
            get { return TCommandCode.REPLACE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class TDeleteCommand : ITCommand
    {
        public ITData Key;

        public TDeleteCommand(ITData key)
        {
            Key = key;
        }

        public int Code
        {
            get { return TCommandCode.DELETE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class TDeleteRangeCommand : ITCommand
    {
        public ITData FromKey;
        public ITData ToKey;

        public TDeleteRangeCommand(ITData fromKey, ITData toKey)
        {
            FromKey = fromKey;
            ToKey = toKey;
        }

        public int Code
        {
            get { return TCommandCode.DELETE_RANGE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class TInsertOrIgnoreCommand : ITCommand
    {
        public ITData Key;
        public ITData Record;

        public TInsertOrIgnoreCommand(ITData key, ITData record)
        {
            Key = key;
            Record = record;
        }

        public int Code
        {
            get { return TCommandCode.INSERT_OR_IGNORE; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class TClearCommand : ITCommand
    {
        public TClearCommand()
        {
        }

        public int Code
        {
            get { return TCommandCode.CLEAR; }
        }

        public bool IsSynchronous
        {
            get { return false; }
        }
    }

    public class TFirstRowCommand : ITCommand
    {
        public KeyValuePair<ITData, ITData>? Row;

        public TFirstRowCommand(KeyValuePair<ITData, ITData>? row)
        {
            Row = row;
        }

        public TFirstRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.FIRST_ROW; }
        }
    }

    public class TLastRowCommand : ITCommand
    {
        public KeyValuePair<ITData, ITData>? Row;

        public TLastRowCommand(KeyValuePair<ITData, ITData>? row)
        {
            Row = row;
        }

        public TLastRowCommand()
            : this(null)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.LAST_ROW; }
        }
    }

    public class TCountCommand : ITCommand
    {
        public long Count;

        public TCountCommand(long count)
        {
            Count = count;
        }

        public TCountCommand()
            : this(0)
        {
        }

        public bool IsSynchronous
        {
            get { return true; }
        }

        public int Code
        {
            get { return TCommandCode.COUNT; }
        }
    }

    public abstract class TOutValueCommand : ITCommand
    {
        private int code;

        public ITData Key;
        public ITData Record;

        public TOutValueCommand(int code, ITData key, ITData record)
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

    public class TTryGetCommand : TOutValueCommand
    {
        public TTryGetCommand(ITData key, ITData record)
            : base(TCommandCode.TRY_GET, key, record)
        {
        }

        public TTryGetCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public abstract class TOutKeyValueCommand : ITCommand
    {
        private int code;

        public ITData Key;
        public KeyValuePair<ITData, ITData>? KeyValue;

        public TOutKeyValueCommand(int code, ITData key, KeyValuePair<ITData, ITData>? keyValue)
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

    public class TFindNextCommand : TOutKeyValueCommand
    {
        public TFindNextCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(TCommandCode.FIND_NEXT, key, keyValue)
        {
        }

        public TFindNextCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class TFindAfterCommand : TOutKeyValueCommand
    {
        public TFindAfterCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(TCommandCode.FIND_AFTER, key, keyValue)
        {
        }

        public TFindAfterCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class TFindPrevCommand : TOutKeyValueCommand
    {
        public TFindPrevCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(TCommandCode.FIND_PREV, key, keyValue)
        {
        }

        public TFindPrevCommand(ITData key)
            : this(key, null)
        {
        }
    }

    public class TFindBeforeCommand : TOutKeyValueCommand
    {
        public TFindBeforeCommand(ITData key, KeyValuePair<ITData, ITData>? keyValue)
            : base(TCommandCode.FIND_BEFORE, key, keyValue)
        {
        }

        public TFindBeforeCommand(ITData key)
            : this(key, null)
        {
        }
    }

    #endregion

    #region IteratorOperations

    public abstract class TIteratorCommand : ITCommand
    {
        private int code;

        public ITData FromKey;
        public ITData ToKey;

        public int PageCount;
        public List<KeyValuePair<ITData, ITData>> List;

        public TIteratorCommand(int code, int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
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

    public class TForwardCommand : TIteratorCommand
    {
        public TForwardCommand(int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
            : base(TCommandCode.FORWARD, pageCount, from, to, list)
        {
        }
    }

    public class TBackwardCommand : TIteratorCommand
    {
        public TBackwardCommand(int pageCount, ITData from, ITData to, List<KeyValuePair<ITData, ITData>> list)
            : base(TCommandCode.BACKWARD, pageCount, from, to, list)
        {
        }
    }

    #endregion

    #region TDescriptor

    public class TXTableDescriptorGetCommand : ITCommand
    {
        public ITDescriptor Descriptor;

        public TXTableDescriptorGetCommand(ITDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code
        {
            get { return TCommandCode.XTABLE_DESCRIPTOR_GET; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    public class TXTableDescriptorSetCommand : ITCommand
    {
        public ITDescriptor Descriptor;

        public TXTableDescriptorSetCommand(ITDescriptor descriptor)
        {
            Descriptor = descriptor;
        }

        public int Code
        {
            get { return TCommandCode.XTABLE_DESCRIPTOR_SET; }
        }

        public bool IsSynchronous
        {
            get { return true; }
        }
    }

    #endregion
}
