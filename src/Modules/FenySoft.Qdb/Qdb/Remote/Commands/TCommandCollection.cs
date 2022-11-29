using FenySoft.Core.Extensions;

namespace FenySoft.Qdb.Remote.Commands
{
    public class TCommandCollection : List<ITCommand>
    {
        public bool AreAllCommon { get; private set; }
        public int CommonAction { get; private set; }

        public TCommandCollection(ITCommand[] operations, bool areAllCommon, int commonCode)
        {
            this.SetArray(operations);

            AreAllCommon = areAllCommon;
            CommonAction = commonCode;
        }

        public TCommandCollection(int capacity)
            : base(capacity)
        {
            AreAllCommon = true;
            CommonAction = TCommandCode.UNDEFINED;
        }

        public new void Add(ITCommand command)
        {
            if (AreAllCommon)
            {
                if (Count == 0)
                    CommonAction = command.Code;

                if (command.Code != CommonAction)
                {
                    AreAllCommon = false;
                    CommonAction = TCommandCode.UNDEFINED;
                }
            }

            base.Add(command);
        }

        public new ITCommand this[int index]
        {
            get
            {
                return base[index];
            }
        }

        public new void Clear()
        {
            base.Clear();

            AreAllCommon = true;
            CommonAction = TCommandCode.UNDEFINED;
        }
    }
}
