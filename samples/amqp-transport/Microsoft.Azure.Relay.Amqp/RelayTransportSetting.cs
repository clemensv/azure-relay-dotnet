// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Azure.Amqp.Transport;

namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Net;
    using System.Globalization;

    public sealed class RelayTransportSetting : TransportSettings
    {
        const int DefaultRelayBacklog = 200;
        const int DefaultRelayAcceptorCount = 1;

        public RelayTransportSetting()
            : base()
        {
            this.ConnectionBacklog = DefaultRelayBacklog;
            this.ListenerAcceptorCount = DefaultRelayAcceptorCount;
        }

        public string RelayNamespace
        {
            get;
            set;
        }

        public string HybridConnectionName
        {
            get;
            set;
        }

        public TokenProvider TokenProvider
        {
            get;
            set;
        }

        public int ConnectionBacklog 
        { 
            get; 
            set; 
        }

        public override TransportInitiator CreateInitiator()
        {
            return new RelayTransportInitiator(this);
        }

// No support for TCP listener in UWP
#if !WINDOWS_UWP
        public override TransportListener CreateListener()
        {
            return new RelayTransportListener(this);
        }
#endif

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}:{1}", this.RelayNamespace, this.HybridConnectionName);
        }
    }
}
