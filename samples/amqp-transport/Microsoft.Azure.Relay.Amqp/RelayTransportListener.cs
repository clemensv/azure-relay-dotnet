// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Amqp;
using Microsoft.Azure.Amqp.Transport;

namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Amqp.Util;

    sealed class RelayTransportListener : TransportListener
    {
        readonly RelayTransportSetting transportSetting;
        HybridConnectionListener listener;

        public RelayTransportListener(RelayTransportSetting transportSetting)
            : base("relay-listener")
        {
            if (transportSetting == null) throw new ArgumentNullException(nameof(transportSetting));
            this.transportSetting = transportSetting;
        }

        protected override bool CloseInternal()
        {
            this.CloseOrAbortListenSockets(false);
            return true;
        }

        protected override void AbortInternal()
        {
            this.CloseOrAbortListenSockets(true);
        }

        protected override void OnListen()
        {
            string listenHost = this.transportSetting.RelayNamespace;
            Fx.Assert(listenHost != null, "RelayNamespace cannot be null!");
            
            this.listener = new HybridConnectionListener(new Uri(string.Format("sb://{0}/{1}", transportSetting.RelayNamespace, transportSetting.HybridConnectionName)), transportSetting.TokenProvider);
            Task.Run((Func<Task>)ListenAsync);
        }

        async Task ListenAsync()
        {
            while (true)
            {
                try
                {
                    var stream = await listener.AcceptConnectionAsync();
                    if (stream != null)
                    {
                        TransportAsyncCallbackArgs args = new TransportAsyncCallbackArgs();
                        args.Transport = new RelayTransport(stream ,transportSetting);
                        args.CompletedSynchronously = false;
                        this.OnTransportAccepted(args);
                    }
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        void CloseOrAbortListenSockets(bool abort)
        {
            if (this.listener != null)
            {
                this.listener.CloseAsync().GetAwaiter().GetResult();
                this.listener = null;
            }
        }
        
    }
}
