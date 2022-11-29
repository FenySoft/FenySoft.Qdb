namespace FenySoft.Qdb.Remote.Commands
{
    public interface ITCommand
    {
        int Code { get; }
        bool IsSynchronous { get; }
    }
}
