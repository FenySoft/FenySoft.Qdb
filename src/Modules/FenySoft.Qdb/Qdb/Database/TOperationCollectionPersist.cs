using FenySoft.Core.Data;
using FenySoft.Core.Persist;
using FenySoft.Qdb.Database.Operations;
using FenySoft.Qdb.WaterfallTree;

namespace FenySoft.Qdb.Database
{
    public class TOperationCollectionPersist : ITPersist<ITOperationCollection>
    {
        public const byte VERSION = 40;

        private readonly Action<BinaryWriter, ITOperation>[] writes;
        private readonly Func<BinaryReader, ITOperation>[] reads;

        public readonly ITPersist<ITData> KeyPersist;
        public readonly ITPersist<ITData> RecordPersist;
        public readonly ITOperationCollectionFactory CollectionFactory;

        public TOperationCollectionPersist(ITPersist<ITData> keyPersist, ITPersist<ITData> recordPersist, ITOperationCollectionFactory collectionFactory)
        {
            KeyPersist = keyPersist;
            RecordPersist = recordPersist;

            writes = new Action<BinaryWriter, ITOperation>[TOperationCode.MAX];
            writes[TOperationCode.REPLACE] = WriteReplaceOperation;
            writes[TOperationCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreOperation;
            writes[TOperationCode.DELETE] = WriteDeleteOperation;
            writes[TOperationCode.DELETE_RANGE] = WriteDeleteRangeOperation;
            writes[TOperationCode.CLEAR] = WriteClearOperation;

            reads = new Func<BinaryReader, ITOperation>[TOperationCode.MAX];
            reads[TOperationCode.REPLACE] = ReadReplaceOperation;
            reads[TOperationCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreOperation;
            reads[TOperationCode.DELETE] = ReadDeleteOperation;
            reads[TOperationCode.DELETE_RANGE] = ReadDeleteRangeOperation;
            reads[TOperationCode.CLEAR] = ReadClearOperation;

            CollectionFactory = collectionFactory;
        }

        #region Write Methods

        private void WriteReplaceOperation(BinaryWriter writer, ITOperation operation)
        {
            TReplaceOperation opr = (TReplaceOperation)operation;

            KeyPersist.Write(writer, opr.FromKey);
            RecordPersist.Write(writer, opr.Record);
        }

        private void WriteInsertOrIgnoreOperation(BinaryWriter writer, ITOperation operation)
        {
            TInsertOrIgnoreOperation opr = (TInsertOrIgnoreOperation)operation;

            KeyPersist.Write(writer, opr.FromKey);
            RecordPersist.Write(writer, opr.Record);
        }

        private void WriteDeleteOperation(BinaryWriter writer, ITOperation operation)
        {
            TDeleteOperation opr = (TDeleteOperation)operation;

            KeyPersist.Write(writer, operation.FromKey);
        }

        private void WriteDeleteRangeOperation(BinaryWriter writer, ITOperation operation)
        {
            TDeleteRangeOperation opr = (TDeleteRangeOperation)operation;

            KeyPersist.Write(writer, operation.FromKey);
            KeyPersist.Write(writer, operation.ToKey);
        }

        private void WriteClearOperation(BinaryWriter writer, ITOperation operation)
        {
            //do nothing
        }

        #endregion

        #region Read Methods

        private ITOperation ReadReplaceOperation(BinaryReader reader)
        {
            ITData key = KeyPersist.Read(reader);
            ITData record = RecordPersist.Read(reader);

            return new TReplaceOperation(key, record);
        }

        private ITOperation ReadInsertOrIgnoreOperation(BinaryReader reader)
        {
            ITData key = KeyPersist.Read(reader);
            ITData record = RecordPersist.Read(reader);

            return new TInsertOrIgnoreOperation(key, record);
        }

        private ITOperation ReadDeleteOperation(BinaryReader reader)
        {
            ITData key = KeyPersist.Read(reader);

            return new TDeleteOperation(key);
        }

        private ITOperation ReadDeleteRangeOperation(BinaryReader reader)
        {
            ITData from = KeyPersist.Read(reader);
            ITData to = KeyPersist.Read(reader);

            return new TDeleteRangeOperation(from, to);
        }

        private ITOperation ReadClearOperation(BinaryReader reader)
        {
            return new TClearOperation();
        }

        #endregion

        public void Write(BinaryWriter writer, ITOperationCollection item)
        {
            writer.Write(VERSION);
            
            writer.Write(item.Count);
            writer.Write(item.AreAllMonotoneAndPoint);

            int commonAction = item.CommonAction;
            writer.Write(commonAction);

            if (commonAction > 0)
            {
                switch (commonAction)
                {
                    case TOperationCode.REPLACE:
                        {
                            for (int i = 0; i < item.Count; i++)
                                WriteReplaceOperation(writer, item[i]);
                        }
                        break;

                    case TOperationCode.INSERT_OR_IGNORE:
                        {
                            for (int i = 0; i < item.Count; i++)
                                WriteInsertOrIgnoreOperation(writer, item[i]);
                        }
                        break;

                    case TOperationCode.DELETE:
                        {
                            for (int i = 0; i < item.Count; i++)
                                WriteDeleteOperation(writer, item[i]);
                        }
                        break;

                    case TOperationCode.DELETE_RANGE:
                        {
                            for (int i = 0; i < item.Count; i++)
                                WriteDeleteRangeOperation(writer, item[i]);
                        }
                        break;

                    case TOperationCode.CLEAR:
                        {
                            for (int i = 0; i < item.Count; i++)
                                WriteClearOperation(writer, item[i]);
                        }
                        break;

                    default:
                        throw new NotSupportedException(commonAction.ToString());
                }
            }
            else
            {
                for (int i = 0; i < item.Count; i++)
                {
                    ITOperation operation = item[i];
                    writer.Write(operation.Code);
                    writes[operation.Code](writer, operation);
                }
            }
        }

        public ITOperationCollection Read(BinaryReader reader)
        {
            if (reader.ReadByte() != VERSION)
                throw new Exception("Invalid TOperationCollectionPersist version.");
            
            int count = reader.ReadInt32();
            bool areAllMonotoneAndPoint = reader.ReadBoolean();
            int commonAction = reader.ReadInt32();

            ITOperation[] array = new ITOperation[count];

            if (commonAction > 0)
            {
                switch (commonAction)
                {
                    case TOperationCode.REPLACE:
                        {
                            for (int i = 0; i < count; i++)
                                array[i] = ReadReplaceOperation(reader);
                        }
                        break;

                    case TOperationCode.INSERT_OR_IGNORE:
                        {
                            for (int i = 0; i < count; i++)
                                array[i] = ReadInsertOrIgnoreOperation(reader);
                        }
                        break;

                    case TOperationCode.DELETE:
                        {
                            for (int i = 0; i < count; i++)
                                array[i] = ReadDeleteOperation(reader);
                        }
                        break;

                    case TOperationCode.DELETE_RANGE:
                        {
                            for (int i = 0; i < count; i++)
                                array[i] = ReadDeleteRangeOperation(reader);
                        }
                        break;

                    case TOperationCode.CLEAR:
                        {
                            for (int i = 0; i < count; i++)
                                array[i] = ReadClearOperation(reader);
                        }
                        break;

                    default:
                        throw new NotSupportedException(commonAction.ToString());
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    int code = reader.ReadInt32();
                    array[i] = reads[code](reader);
                }
            }

            ITOperationCollection operations = CollectionFactory.Create(array, commonAction, areAllMonotoneAndPoint);

            return operations;
        }
    }
}