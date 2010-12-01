using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ServiceStack.Redis;

namespace NHibernate.Caches.Redis
{
    public class DisposableClient : IDisposable
    {
        private CustomRedisClient client;
        private PooledRedisClientManager clientManager;
        public DisposableClient(PooledRedisClientManager clientManager)
        {
             this.clientManager = clientManager;
              if (clientManager != null)
                client = (CustomRedisClient)clientManager.GetClient();
        }
        public CustomRedisClient Client { get { return client;} }
        public void Dispose()
        {
            if (client != null)
                clientManager.DisposeClient(client);
        }
    }

    

}
