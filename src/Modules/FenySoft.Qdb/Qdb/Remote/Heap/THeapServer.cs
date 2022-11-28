using FenySoft.Qdb.WaterfallTree;
using FenySoft.Remote;

namespace FenySoft.Qdb.Remote.Heap
{
  public class THeapServer
  {
    private CancellationTokenSource FCancellationTokenSource;
    private Thread? FWorker;

    public readonly IHeap? Heap;
    public readonly TTcpServer? TcpServer;

    public THeapServer(IHeap? AHeap, TTcpServer? ATcpServer)
    {
      Heap = AHeap ?? throw new ArgumentNullException("AHeap");
      TcpServer = ATcpServer ?? throw new ArgumentNullException("ATcpServer");
    }

    public THeapServer(IHeap? AHeap, int APort = 7183) : this(AHeap, new TTcpServer(APort))
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

            var code = (RemoteHeapCommandCodes)reader.ReadByte();

            switch (code)
            {
              case RemoteHeapCommandCodes.ObtainHandle:
                ObtainHandleCommand.WriteResponse(writer, Heap.ObtainNewHandle());
                break;

              case RemoteHeapCommandCodes.ReleaseHandle:
              {
                var handle = ReleaseHandleCommand.ReadRequest(reader).Handle;
                Heap.Release(handle);
                break;
              }

              case RemoteHeapCommandCodes.HandleExist:
              {
                long handle = HandleExistCommand.ReadRequest(reader).Handle;
                HandleExistCommand.WriteResponse(writer, Heap.Exists(handle));
                break;
              }

              case RemoteHeapCommandCodes.WriteCommand:
                var cmd = WriteCommand.ReadRequest(reader);
                Heap.Write(cmd.Handle, cmd.Buffer, cmd.Index, cmd.Count);

                break;

              case RemoteHeapCommandCodes.ReadCommand:
              {
                var handle = ReadCommand.ReadRequest(reader)
                                        .Handle;

                ReadCommand.WriteResponse(writer, Heap.Read(handle));

                break;
              }

              case RemoteHeapCommandCodes.CommitCommand:
                Heap.Commit();

                break;

              case RemoteHeapCommandCodes.CloseCommand:
                Heap.Close();

                break;

              case RemoteHeapCommandCodes.SetTag:
                Heap.Tag = SetTagCommand.ReadRequest(reader)
                                        .Tag;

                break;

              case RemoteHeapCommandCodes.GetTag:
                GetTagCommand.WriteResponse(writer, Heap.Tag);

                break;

              case RemoteHeapCommandCodes.Size:
                SizeCommand.WriteResponse(writer, Heap.Size);

                break;

              case RemoteHeapCommandCodes.DataBaseSize:
                DataBaseSizeCommand.WriteResponse(writer, Heap.DataSize);

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