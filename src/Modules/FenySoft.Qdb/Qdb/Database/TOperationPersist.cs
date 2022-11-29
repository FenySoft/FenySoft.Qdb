namespace FenySoft.Qdb.Database
{
    //public class OperationPersist : ITPersist<ITOperation>
    //{
    //    private ITPersist<ITData> KeyPersist;
    //    private ITPersist<ITData> RecordPersist;

    //    private Action<BinaryWriter, ITOperation>[] writes;
    //    private Func<BinaryReader, ITOperation>[] reads;

    //    public OperationPersist(ITPersist<ITData> keyPersist, ITPersist<ITData> recordPersist)
    //    {
    //        KeyPersist = keyPersist;
    //        RecordPersist = recordPersist;

    //        writes = new Action<BinaryWriter, ITOperation>[TOperationCode.MAX];
    //        writes[TOperationCode.REPLACE] = WriteReplaceOperation;
    //        writes[TOperationCode.INSERT_OR_IGNORE] = WriteInsertOrIgnoreOperation;
    //        writes[TOperationCode.DELETE] = WriteDeleteOperation;
    //        writes[TOperationCode.DELETE_RANGE] = WriteDeleteRangeOperation;
    //        writes[TOperationCode.CLEAR] = WriteClearOperation;

    //        reads = new Func<BinaryReader, ITOperation>[TOperationCode.MAX];
    //        reads[TOperationCode.REPLACE] = ReadReplaceOperation;
    //        reads[TOperationCode.INSERT_OR_IGNORE] = ReadInsertOrIgnoreOperation;
    //        reads[TOperationCode.DELETE] = ReadDeleteOperation;
    //        reads[TOperationCode.DELETE_RANGE] = ReadDeleteRangeOperation;
    //        reads[TOperationCode.CLEAR] = ReadClearOperation;
    //    }

    //    #region Write Methods

    //    private void WriteReplaceOperation(BinaryWriter writer, ITOperation operation)
    //    {
    //        TReplaceOperation opr = (TReplaceOperation)operation;

    //        KeyPersist.Write(writer, opr.FromKey);
    //        RecordPersist.Write(writer, opr.Record);
    //    }

    //    private void WriteInsertOrIgnoreOperation(BinaryWriter writer, ITOperation operation)
    //    {
    //        TInsertOrIgnoreOperation opr = (TInsertOrIgnoreOperation)operation;

    //        KeyPersist.Write(writer, opr.FromKey);
    //        RecordPersist.Write(writer, opr.Record);
    //    }

    //    private void WriteDeleteOperation(BinaryWriter writer, ITOperation operation)
    //    {
    //        TDeleteOperation opr = (TDeleteOperation)operation;
            
    //        KeyPersist.Write(writer, operation.FromKey);
    //    }

    //    private void WriteDeleteRangeOperation(BinaryWriter writer, ITOperation operation)
    //    {
    //        TDeleteRangeOperation opr = (TDeleteRangeOperation)operation;
            
    //        KeyPersist.Write(writer, operation.FromKey);
    //        KeyPersist.Write(writer, operation.ToKey);
    //    }

    //    private void WriteClearOperation(BinaryWriter writer, ITOperation operation)
    //    {
    //        //do nothing
    //    }

    //    #endregion

    //    #region Read Methods

    //    private ITOperation ReadReplaceOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);
    //        ITData record = RecordPersist.Read(reader);

    //        return new TReplaceOperation(key, record);
    //    }

    //    private ITOperation ReadInsertOrIgnoreOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);
    //        ITData record = RecordPersist.Read(reader);

    //        return new TInsertOrIgnoreOperation(key, record);
    //    }

    //    private ITOperation ReadDeleteOperation(BinaryReader reader)
    //    {
    //        ITData key = KeyPersist.Read(reader);

    //        return new TDeleteOperation(key);
    //    }

    //    private ITOperation ReadDeleteRangeOperation(BinaryReader reader)
    //    {
    //        ITData from = KeyPersist.Read(reader);
    //        ITData to = KeyPersist.Read(reader);

    //        return new TDeleteRangeOperation(from, to);
    //    }

    //    private ITOperation ReadClearOperation(BinaryReader reader)
    //    {
    //        return new TClearOperation();
    //    }

    //    #endregion

    //    public void Write(BinaryWriter writer, ITOperation item)
    //    {
    //        var code = item.Code;

    //        writer.Write(code);
    //        writes[code](writer, item);
    //    }

    //    public ITOperation Read(BinaryReader reader)
    //    {
    //        int code = reader.ReadInt32();
    //        ITOperation operation = reads[code](reader);

    //        return operation;
    //    }
    //}
}
