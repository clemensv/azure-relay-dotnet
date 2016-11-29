// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.


namespace Microsoft.Azure.Relay.Amqp
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Azure.Amqp.Transport;

    static class Extensions
    {
        public static bool WaitAsync(this Task task, TransportAsyncCallbackArgs args, Action<TransportAsyncCallbackArgs, Task> resultAction)
        {
            bool pending = !task.IsCompleted;
            if (pending)
            {
                task.ContinueWith(t =>
                {
                    args.CompletedSynchronously = false;
                    if (t.IsFaulted)
                    {
                        args.Exception = t.Exception;
                    }
                    else if (t.IsCanceled)
                    {
                        args.Exception = new OperationCanceledException();
                    }
                    else
                    {
                        resultAction?.Invoke(args, t);
                    }

                    args.CompletedCallback(args);
                });
            }
            else
            {
                args.CompletedSynchronously = true;
                resultAction?.Invoke(args, task);
            }

            return pending;
        }
    }
}
