namespace FenySoft.Qdb.Database
{
    public static class TStructureType
    {
        //do not change
        public const int RESERVED = 0;

        public const int XTABLE = 1;
        public const int XFILE = 2;

        public static bool IsValid(int type)
        {
            if (type == XTABLE || type == XFILE)
                return true;

            return false;
        }
    }
}
