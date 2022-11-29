namespace FenySoft.Qdb.Remote.Commands
{
    public interface ITCommandCollectionPersist
    {
        void Write(BinaryWriter writer, TCommandCollection collection);
        TCommandCollection Read(BinaryReader reader);
    }
}
