﻿namespace FenySoft.Qdb.Remote.Commands
{
    public interface ICommandCollectionPersist
    {
        void Write(BinaryWriter writer, CommandCollection collection);
        CommandCollection Read(BinaryReader reader);
    }
}
