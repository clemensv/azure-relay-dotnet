// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Amqp.Transport;

namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Net;
    using System.Net.Sockets;
    using System.Threading.Tasks;
    using Azure.Amqp.Util;

    sealed class RelayTransportInitiator : TransportInitiator
    {
        readonly RelayTransportSetting transportSetting;
        TransportAsyncCallbackArgs callbackArgs;
        private HybridConnectionClient hcc;

        internal RelayTransportInitiator(RelayTransportSetting transportSetting)
        {
            this.transportSetting = transportSetting;
        }

        public override bool ConnectAsync(TimeSpan timeout, TransportAsyncCallbackArgs callbackArgs)
        {
            this.callbackArgs = callbackArgs;
            this.hcc = new HybridConnectionClient(new Uri(string.Format("sb://{0}/{1}", this.transportSetting.RelayNamespace, this.transportSetting.HybridConnectionName)), this.transportSetting.TokenProvider);
            this.hcc.CreateConnectionAsync().ContinueWith(CompleteConnect);
            return false;
        }


        void CompleteConnect(Task<HybridConnectionStream> connectTask)
        {
            TransportBase transport = null;
            if (!connectTask.IsFaulted)
            {
                transport = new RelayTransport(connectTask.Result, this.transportSetting);
            }

            this.callbackArgs.CompletedSynchronously = false;
            this.callbackArgs.Exception = connectTask.IsFaulted?connectTask.Exception:null;
            this.callbackArgs.Transport = transport;

            this.callbackArgs.CompletedCallback(this.callbackArgs);
        }
    }
}
