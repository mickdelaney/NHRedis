using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace NHibernate.Caches.Redis
{
    public class RedisGarbageCollector
    {
        // clean-up thread

        private int gcRefCount = 0;
        private bool shouldStop = true;
        private Thread garbageThread;

        public void collectGarbage()
        {
            while (!shouldStop)
            {
                Console.WriteLine("worker thread: working...");
            }
            Console.WriteLine("worker thread: terminating gracefully.");
        }


        public void startGarbageCollector()
        {
            lock (this)
            {
                shouldStop = false;
                garbageThread = new Thread(collectGarbage);
            }

        }
        void stopGarbageCollector()
        {
            lock (this)
            {
                gcRefCount--;
                if (gcRefCount == 0)
                {
                    //kill thread
                    // Use the Join method to block the current thread 
                    // until the object's thread terminates.
                    garbageThread.Join();
                    garbageThread = null;

                }
            }

        }
    }
}
