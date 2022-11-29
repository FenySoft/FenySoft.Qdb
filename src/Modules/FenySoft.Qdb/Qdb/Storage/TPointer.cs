namespace FenySoft.Qdb.Storage
{
    public class TPointer
    {
        public long Version;
        public TPtr Ptr;

        public bool IsReserved;
        public int RefCount;

        public TPointer(long version, TPtr ptr)
        {
            Version = version;
            Ptr = ptr;
        }

        public void Serialize(BinaryWriter writer)
        {
            writer.Write(Version);
            Ptr.Serialize(writer);
        }

        public static TPointer Deserialize(BinaryReader reader)
        {
            long version = reader.ReadInt64();
            TPtr ptr = TPtr.Deserialize(reader);

            return new TPointer(version, ptr);
        }

        public override string ToString()
        {
            return String.Format("Version {0}, TPtr {1}", Version, Ptr);
        }
    }
}
