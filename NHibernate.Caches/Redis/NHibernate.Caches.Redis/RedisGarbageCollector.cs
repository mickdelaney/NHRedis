using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Redis;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Garbage collection class. Runs its own thread, doing BLPOP on garbage key list, then deleting all keys in list
    /// and then deleting list.
    /// </summary>
    public class RedisGarbageCollector
    {
        private int gcRefCount = 0;
        private bool shouldStop = true;
        private Thread garbageThread;
        private RedisClient client;
        private string host;
        private int port;

        public RedisGarbageCollector(string host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public void collectGarbage()
        {
            while (!shouldStop)
            {
                //BLPOP from garbage list
                //run through keys, expiring all keys in list
                string listKey = client.BlockingPopItemFromList(RedisNamespace.namespacesGarbageKey, TimeSpan.FromSeconds(1));
                if (listKey != null)
                {
                    string key = client.PopItemFromSet(listKey);
                    while ( key != null && !shouldStop)
                    {
                        client.Expire(key, 0);
                        key = client.PopItemFromSet(listKey);
                    }
                    client.Expire(listKey,0);
               }
            }
        }

        // start ye olde garbage collection thread
        public void start()
        {
            lock (this)
            {
                gcRefCount++;
                if (gcRefCount == 1)
                {
                    shouldStop = false;
                    client = new RedisClient(host, port);
                    garbageThread = new Thread(collectGarbage);
                    garbageThread.Start();
                    
                }
 
            }

        }
        //stop ye olde thread
        public void stop()
        {
            lock (this)
            {
                gcRefCount--;
                if (gcRefCount == 0)
                {
                    //kill thread
                    shouldStop = true;
                    // Use the Join method to block the current thread 
                    // until the object's thread terminates.
                    garbageThread.Join();
                    garbageThread = null;
                    client.Dispose();
                    client = null;

                }
            }
        }
    }
}
