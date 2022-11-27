using FenySoft.Core.Data;

namespace FenySoft.Qdb.WaterfallTree
{
    public enum OperationScope : byte
    {
        Point,
        Range,
        Overall
    }

    public interface IOperation
    {
        int Code { get; }
        OperationScope Scope { get; }

        ITData FromKey { get; }
        ITData ToKey { get; }
    }
}