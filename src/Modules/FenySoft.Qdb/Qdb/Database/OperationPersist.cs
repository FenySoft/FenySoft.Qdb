namespace FenySoft.Qdb.Database
{
    //public class OperationPersist : ITPersist<IOperation>
    //{
    //    private ITPersist<ITData> KeyPersist;
    //    private ITPersist<ITData> RecordPersist;

    //    private Action<BinaryWriter, IOperation>[] writes;
    //    private Func<BinaryReader, IOperation>[] reads;

    //    public OperationPersist(ITPersist<ITData> keyPersist, ITPersist<ITData> recordPersist)
    //    {
    //        KeyPersist = keyPersist;
    //        RecordPersist = recordPersist;

    //        writes = new Action<BinaryWriter, IOperation>[OperationCode.MAX];
    //        writes[OperationCode.REPLACE] = WriteReplaceOperation;
    //        writes[OperationCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreOperation;
    //        writes[OperationCode.DELETE] = WriteDeleteOperation;
    //        writes[OperationCode.DELETE_RANGE] = WriteDeleteRangeOperation;
    //        writes[OperationCode.CLEAR] = WriteClearOperation;

    //        reads = new Func<BinaryReader, IOperation>[OperationCode.MAX];
    //        reads[OperationCode.REPLACE] = ReadReplaceOperation;
    //        reads[OperationCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreOperation;
    //        reads[OperationCode.DELETE] = ReadDeleteOperation;
    //        reads[OperationCode.DELETE_RANGE] = ReadDeleteRangeOperation;
    //        reads[OperationCode.CLEAR] = ReadClearOperation;
    //    }

    //    #region Write Methods

    //    private void WriteReplaceOperation(BinaryWriter writer, IOperation operation)
    //    {
    //        ReplaceOperation opr = (ReplaceOperation)operation;

    //        KeyPersist.Write(writer, opr.FromKey);
    //        RecordPersist.Write(writer, opr.Record);
    //    }

    //    private void WriteInsertOrIgnoreOperation(BinaryWriter writer, IOperation operation)
    //    {
    //        InsertOrIgnoreOperation opr = (InsertOrIgnoreOperation)operation;

    //        KeyPersist.Write(writer, opr.FromKey);
    //        RecordPersist.Write(writer, opr.Record);
    //    }

    //    private void WriteDeleteOperation(BinaryWriter writer, IOperation operation)
    //    {
    //        DeleteOperation opr = (DeleteOperation)operation;
            
    //        KeyPersist.Write(writer, operation.FromKey);
    //    }

    //    private void WriteDeleteRangeOperation(BinaryWriter writer, IOperation operation)
    //    {
    //        DeleteRangeOperation opr = (DeleteRangeOperation)operation;
            
    //        KeyPersist.Write(writer, operation.FromKey);
    //        KeyPersist.Write(writer, operation.ToKey);
    //    }

    //    private void WriteClearOperation(BinaryWriter writer, IOperation operation)
    //    {
    //        //do nothing
    //    }

    //    #endregion

    //    #region Read Methods

    //    private IOperation ReadReplaceOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);
    //        ITData record = RecordPersist.Read(reader);

    //        return new ReplaceOperation(key, record);
    //    }

    //    private IOperation ReadInsertOrIgnoreOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);
    //        ITData record = RecordPersist.Read(reader);

    //        return new InsertOrIgnoreOperation(key, record);
    //    }

    //    private IOperation ReadDeleteOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);

    //        return new DeleteOperation(key);
    //    }

    //    private IOperation ReadDeleteRangeOperation(BinaryReader reader)
    //    {
    //        ITData from = KeyPersist.Read(reader);
    //        ITData to = KeyPersist.Read(reader);

    //        return new DeleteRangeOperation(from, to);
    //    }

    //    private IOperation ReadClearOperation(BinaryReader reader)
    //    {
    //        return new ClearOperation();
    //    }

    //    #endregion

    //    public void Write(BinaryWriter writer, IOperation item)
    //    {
    //        var code = item.Code;

    //        writer.Write(code);
    //        writes[code](writer, item);
    //    }

    //    public IOperation Read(BinaryReader reader)
    //    {
    //        int code = reader.ReadInt32();
    //        IOperation operation = reads[code](reader);

    //        return operation;
    //    }
    //}
}
