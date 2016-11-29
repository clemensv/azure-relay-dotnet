// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp.Transport;

    sealed class RelayTransportListener : TransportListener
    {
        readonly RelayTransportSetting transportSetting;
        HybridConnectionListener listener;

        public RelayTransportListener(RelayTransportSetting transportSetting)
            : base("relay-listener")
        {
            if (transportSetting == null)
            {
                throw new ArgumentNullException(nameof(transportSetting));
            }

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
            
            this.listener = new HybridConnectionListener(new Uri(string.Format("sb://{0}/{1}", transportSetting.RelayNamespace, transportSetting.HybridConnectionName)), transportSetting.TokenProvider);
            Task.Run((Func<Task>)ListenAsync);
        }

        async Task ListenAsync()
        {
            while (this.listener != null)
            {
                try
                {
                    var stream = await listener.AcceptConnectionAsync();
                    if (stream != null)
                    {
                        TransportAsyncCallbackArgs args = new TransportAsyncCallbackArgs();
                        args.Transport = new RelayTransport(stream, transportSetting);
                        args.CompletedSynchronously = false;
                        this.OnTransportAccepted(args);
                    }
                }
                catch (Exception)
                {
                    // keep accepting as long as the listener is not closed
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
