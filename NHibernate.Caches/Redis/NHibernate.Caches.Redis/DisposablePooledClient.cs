using System;
using ServiceStack.Redis;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Manage a client acquired from the PooledRedisClientManager
    /// Dispose method will release the client back to the pool.
    /// </summary>
    public class DisposablePooledClient : IDisposable
    {
        private readonly CustomRedisClient client;
        private readonly PooledRedisClientManager clientManager;

        /// <summary>
        /// wrap the acquired client
        /// </summary>
        /// <param name="clientManager"></param>
        public DisposablePooledClient(PooledRedisClientManager clientManager)
        {
             this.clientManager = clientManager;
              if (clientManager != null)
                client = (CustomRedisClient)clientManager.GetClient();
        }
        
        /// <summary>
        /// access the wrapped client
        /// </summary>
        public CustomRedisClient Client { get { return client;} }
 
        /// <summary>
        /// release the wrapped client back to the pool
        /// </summary>
        public void Dispose()
        {
            if (client != null)
                clientManager.DisposeClient(client);
        }
    }
}
