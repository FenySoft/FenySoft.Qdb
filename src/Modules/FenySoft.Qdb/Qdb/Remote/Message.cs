using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;
using FenySoft.Qdb.Remote.Commands;

namespace FenySoft.Qdb.Remote
{
    ///<summary>
    ///--------------------- Message Exchange Protocol
    ///
    ///--------------------- Comments-----------------------------------
    ///Format           : binary
    ///Byte style       : LittleEndian
    ///String Encoding  : Unicode (UTF-8) 
    ///String format    : string int size compressed with 7-bit encoding, byte[] Unicode (UTF-8)
    ///
    ///------------------------------------------------------------------
    ///ID                : Long ID
    ///     
    ///Commands          : CommandCollection
    ///
    ///</summary>    
    public class Message
    {
        public ITDescriptor Description { get; private set; }
        public CommandCollection Commands { get; private set; }

        private static KeyValuePair<long, ITDescriptor> PreviousRecord = new KeyValuePair<long, ITDescriptor>(-1, null);

        public Message(ITDescriptor description, CommandCollection commands)
        {
            Description = description;
            Commands = commands;
        }

        public void Serialize(BinaryWriter writer)
        {
            long ID = Description.ID;

            writer.Write(ID);

            CommandPersist persist = ID > 0 ? new CommandPersist(new TDataPersist(Description.KeyType, null, AllowNull.OnlyMembers), new TDataPersist(Description.RecordType, null, AllowNull.OnlyMembers)) : new CommandPersist(null, null);
            CommandCollectionPersist commandsPersist = new CommandCollectionPersist(persist);

            commandsPersist.Write(writer, Commands);
        }

        public static Message Deserialize(BinaryReader reader, Func<long, ITDescriptor> find)
        {
            long ID = reader.ReadInt64();

            ITDescriptor description = null;
            CommandPersist persist = new CommandPersist(null, null);

            if (ID > 0)
            {
                try
                {
                    description = PreviousRecord.Key == ID ? PreviousRecord.Value : find(ID);
                    persist = new CommandPersist(new TDataPersist(description.KeyType, null, AllowNull.OnlyMembers), new TDataPersist(description.RecordType, null, AllowNull.OnlyMembers));
                }
                catch (Exception exc)
                {
                    throw new Exception("Cannot find description with the specified ID");
                }

                if (PreviousRecord.Key != ID)
                    PreviousRecord = new KeyValuePair<long, ITDescriptor>(ID, description);
            }
            
            var commandsPersist = new CommandCollectionPersist(persist);
            CommandCollection commands = commandsPersist.Read(reader);

            return new Message(description, commands);
        }
    }
}