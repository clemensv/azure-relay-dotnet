// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp;
    using Microsoft.Azure.Amqp.Transport;

    sealed class RelayTransport : TransportBase
    {
        readonly HybridConnectionStream stream;
        readonly EndPoint localEndPoint;
        readonly EndPoint remoteEndPoint;

        public RelayTransport(HybridConnectionStream stream, RelayTransportSetting _transportSetting)
            : base("relay")
        {
            this.stream = stream;
            this.localEndPoint = new IPEndPoint(IPAddress.Any, -1); // create a dummy one
            this.remoteEndPoint = new DnsEndPoint(_transportSetting.RelayNamespace, 443);
        }

        public override EndPoint LocalEndPoint
        {
            get
            {
                return this.localEndPoint;
            }
        }

        public override EndPoint RemoteEndPoint
        {
            get
            {
                return this.remoteEndPoint;
            }
        }

        public override void SetMonitor(ITransportMonitor monitor)
        {
        }

        public sealed override bool WriteAsync(TransportAsyncCallbackArgs args)
        {
            Task writeTask;
            if (args.Buffer != null)
            {
                writeTask = this.stream.WriteAsync(args.Buffer, args.Offset, args.Count);
            }
            else
            {
                int size = args.ByteBufferList.Sum(b => b.Length);
                var dstBuffer = new ByteBuffer(size, false);
                foreach (ByteBuffer buffer in args.ByteBufferList)
                {
                    Buffer.BlockCopy(buffer.Buffer, buffer.Offset, dstBuffer.Buffer, dstBuffer.WritePos, buffer.Length);
                    dstBuffer.Append(buffer.Length);
                    buffer.Dispose();
                }

                writeTask = this.stream.WriteAsync(dstBuffer.Buffer, dstBuffer.Offset, dstBuffer.Length);
            }

            return writeTask.WaitAsync(args, (a, t) => a.BytesTransfered = a.Count);
        }

        public sealed override bool ReadAsync(TransportAsyncCallbackArgs args)
        {
            Task<int> readTask = this.stream.ReadAsync(args.Buffer, args.Offset, args.Count);
            return readTask.WaitAsync(args, (a, t) => a.BytesTransfered = ((Task<int>)t).Result);
        }

        protected override bool CloseInternal()
        {
            this.stream.Shutdown();
            this.stream.Dispose();
            return true;
        }

        protected override void AbortInternal()
        {
            this.stream.Dispose();
        }
    }
}
