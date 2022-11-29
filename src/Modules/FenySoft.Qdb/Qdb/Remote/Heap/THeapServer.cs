using FenySoft.Qdb.WaterfallTree;
using FenySoft.Remote;

namespace FenySoft.Qdb.Remote.Heap
{
  public class THeapServer
  {
    private CancellationTokenSource FCancellationTokenSource;
    private Thread? FWorker;

    public readonly ITHeap? Heap;
    public readonly TTcpServer? TcpServer;

    public THeapServer(ITHeap? AHeap, TTcpServer? ATcpServer)
    {
      Heap = AHeap ?? throw new ArgumentNullException("AHeap");
      TcpServer = ATcpServer ?? throw new ArgumentNullException("ATcpServer");
    }

    public THeapServer(ITHeap? AHeap, int APort = 7183) : this(AHeap, new TTcpServer(APort))
    {
    }

    public void Start()
    {
      Stop();

      FCancellationTokenSource = new CancellationTokenSource();
      FWorker = new Thread(DoWork);
      FWorker.Start();
    }

    public void Stop()
    {
      if (!IsWorking)
        return;

      FCancellationTokenSource.Cancel(false);
      Thread thread = FWorker;

      if (thread != null)
      {
        if (!thread.Join(5000))
          thread.Abort();
      }

      Heap.Close();
    }

    private void DoWork()
    {
      try
      {
        TcpServer.Start();

        while (!FCancellationTokenSource.Token.IsCancellationRequested)
        {
          try
          {
            var order = TcpServer.RecievedPacketsTake(FCancellationTokenSource.Token);

            BinaryReader reader = new BinaryReader(order.Value.Request);
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            var code = (TRemoteHeapCommandCodes)reader.ReadByte();

            switch (code)
            {
              case TRemoteHeapCommandCodes.ObtainHandle:
                TObtainHandleCommand.WriteResponse(writer, Heap.ObtainNewHandle());
                break;

              case TRemoteHeapCommandCodes.ReleaseHandle:
              {
                var handle = TReleaseHandleCommand.ReadRequest(reader).Handle;
                Heap.Release(handle);
                break;
              }

              case TRemoteHeapCommandCodes.HandleExist:
              {
                long handle = THandleExistCommand.ReadRequest(reader).Handle;
                THandleExistCommand.WriteResponse(writer, Heap.Exists(handle));
                break;
              }

              case TRemoteHeapCommandCodes.WriteCommand:
                var cmd = TWriteCommand.ReadRequest(reader);
                Heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);

                break;

              case TRemoteHeapCommandCodes.ReadCommand:
              {
                var handle = TReadCommand.ReadRequest(reader)
                                        .Handle;

                TReadCommand.WriteResponse(writer, Heap.Read(handle));

                break;
              }

              case TRemoteHeapCommandCodes.CommitCommand:
                Heap.Commit();

                break;

              case TRemoteHeapCommandCodes.CloseCommand:
                Heap.Close();

                break;

              case TRemoteHeapCommandCodes.SetTag:
                Heap.Tag = TSetTagCommand.ReadRequest(reader)
                                        .Tag;

                break;

              case TRemoteHeapCommandCodes.GetTag:
                TGetTagCommand.WriteResponse(writer, Heap.Tag);

                break;

              case TRemoteHeapCommandCodes.Size:
                TSizeCommand.WriteResponse(writer, Heap.Size);

                break;

              case TRemoteHeapCommandCodes.DataBaseSize:
                TDataBaseSizeCommand.WriteResponse(writer, Heap.DataSize);

                break;

              default:
                break;
            }

            ms.Position = 0;
            order.Value.Response = ms;
            order.Key.FPendingPackets.Add(order.Value);
          }
          catch (OperationCanceledException)
          {
            break;
          }
          catch (Exception exc)
          {
            TcpServer.LogError(exc);
          }
        }
      }
      catch (Exception exc)
      {
        TcpServer.LogError(exc);
      }
      finally
      {
        TcpServer.Stop();
        FWorker = null;
      }
    }

    public bool IsWorking { get { return FWorker != null; } }

    public int ClientsCount { get { return TcpServer.ConnectionsCount; } }
  }
}