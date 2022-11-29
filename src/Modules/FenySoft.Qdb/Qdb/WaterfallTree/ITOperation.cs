using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public enum TOperationScope : byte
    {
        Point,
        Range,
        Overall
    }

    public interface ITOperation
    {
        int Code { get; }
        TOperationScope Scope { get; }

        ITData FromKey { get; }
        ITData ToKey { get; }
    }
}