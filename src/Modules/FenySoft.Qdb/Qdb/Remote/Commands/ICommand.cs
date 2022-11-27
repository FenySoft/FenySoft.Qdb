namespace FenySoft.Qdb.Remote.Commands
{
    public interface ICommand
    {
        int Code { get; }
        bool IsSynchronous { get; }
    }
}
