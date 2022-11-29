using FenySoft.Core.Data;
using FenySoft.Qdb.WaterfallTree;
using FenySoft.Qdb.Remote.Commands;

namespace FenySoft.Qdb.Remote
{
    ///<summary>
    ///--------------------- TMessage Exchange Protocol
    ///
    ///--------------------- Comments-----------------------------------
    /// Format           : binary
    /// Byte style       : LittleEndian
    /// String Encoding  : Unicode (UTF-8) 
    /// String format    : string int size compressed with 7-bit encoding, byte[] Unicode (UTF-8)
    ///
    ///------------------------------------------------------------------
    /// ID                : Long ID
    ///     
    /// Commands          : TCommandCollection
    ///
    ///</summary>    
    public class TMessage
    {
        public ITDescriptor Description { get; private set; }
        public TCommandCollection Commands { get; private set; }

        private static KeyValuePair<long, ITDescriptor> PreviousRecord = new KeyValuePair<long, ITDescriptor>(-1, null);

        public TMessage(ITDescriptor description, TCommandCollection commands)
        {
            Description = description;
            Commands = commands;
        }

        public void Serialize(BinaryWriter writer)
        {
            long ID = Description.ID;

            writer.Write(ID);

            TCommandPersist persist = ID > 0 ? new TCommandPersist(new TDataPersist(Description.KeyType, null, AllowNull.OnlyMembers), new TDataPersist(Description.RecordType, null, AllowNull.OnlyMembers)) : new TCommandPersist(null, null);
            TCommandCollectionPersist commandsPersist = new TCommandCollectionPersist(persist);

            commandsPersist.Write(writer, Commands);
        }

        public static TMessage Deserialize(BinaryReader reader, Func<long, ITDescriptor> find)
        {
            long ID = reader.ReadInt64();

            ITDescriptor description = null;
            TCommandPersist persist = new TCommandPersist(null, null);

            if (ID > 0)
            {
                try
                {
                    description = PreviousRecord.Key == ID ? PreviousRecord.Value : find(ID);
                    persist = new TCommandPersist(new TDataPersist(description.KeyType, null, AllowNull.OnlyMembers), new TDataPersist(description.RecordType, null, AllowNull.OnlyMembers));
                }
                catch (Exception exc)
                {
                    throw new Exception("Cannot find description with the specified ID");
                }

                if (PreviousRecord.Key != ID)
                    PreviousRecord = new KeyValuePair<long, ITDescriptor>(ID, description);
            }
            
            var commandsPersist = new TCommandCollectionPersist(persist);
            TCommandCollection commands = commandsPersist.Read(reader);

            return new TMessage(description, commands);
        }
    }
}