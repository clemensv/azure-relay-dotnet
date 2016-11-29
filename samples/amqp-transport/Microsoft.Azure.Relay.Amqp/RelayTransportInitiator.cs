// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp.Transport;

    sealed class RelayTransportInitiator : TransportInitiator
    {
        readonly RelayTransportSetting transportSetting;
        HybridConnectionClient hcc;

        internal RelayTransportInitiator(RelayTransportSetting transportSetting)
        {
            this.transportSetting = transportSetting;
        }

        public override bool ConnectAsync(TimeSpan timeout, TransportAsyncCallbackArgs callbackArgs)
        {
            this.hcc = new HybridConnectionClient(new Uri(string.Format("sb://{0}/{1}", this.transportSetting.RelayNamespace, this.transportSetting.HybridConnectionName)), this.transportSetting.TokenProvider);
            Task<HybridConnectionStream> task = this.hcc.CreateConnectionAsync();
            return task.WaitAsync(callbackArgs, (a, t) => a.Transport = new RelayTransport(((Task<HybridConnectionStream>)t).Result, this.transportSetting));
        }
    }
}
