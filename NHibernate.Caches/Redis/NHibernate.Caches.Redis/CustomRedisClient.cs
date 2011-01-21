using System.Collections.Generic;
using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections;
using System.IO;
using System;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClient : RedisClient
    {
        private readonly BinaryFormatter _bf = new BinaryFormatter();
 
        public CustomRedisClient(string host, int port)
			: base(host, port)
		{
		}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private static double CalculateLockExire(TimeSpan ts, int timeout)
        {
           return ts.TotalSeconds + timeout + 1;
        }
        
        /// <summary>
        /// acquire distributed, non-reentrant lock on key
        /// </summary>
        /// <param name="key">global key for this lock</param>
        /// <param name="acquisitionTimeout">timeout for acquiring lock</param>
        /// <param name="lockTimeout">timeout for lock, in seconds (stored as value against lock key) </param>
        public double Lock(string key, int acquisitionTimeout, int lockTimeout)
        {
            int sleepIfLockSet = 200;
            acquisitionTimeout *= 1000; //convert to ms
            int tryCount = acquisitionTimeout/sleepIfLockSet + 1;
    
            var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            double lockExpire = CalculateLockExire(ts, lockTimeout);
            int wasSet = SetNX(key, Serialize(lockExpire));
            int totalTime = 0;
            while (wasSet == 0 && totalTime < acquisitionTimeout)
            {
                int count = 0;
                while (wasSet == 0 && count < tryCount && totalTime < acquisitionTimeout)
                {
                    System.Threading.Thread.Sleep(sleepIfLockSet);
                    totalTime += sleepIfLockSet;
                    ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                    lockExpire = CalculateLockExire(ts, lockTimeout);
                    wasSet = SetNX(key, Serialize(lockExpire));
                    count++;
                }
                // acquired lock!
                if (wasSet != 0) break;

                // handle possibliity of crashed client still holding the lock
                using (var pipe = CreatePipeline())
                {
                    object lockValRaw = null;
                    pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
                    pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockValRaw = Deserialize((x)));
                    pipe.Flush();

                    double lockVal = 0;
                    if (lockValRaw != null)
                        lockVal = (double) lockValRaw;

                    // if lock value is null, or expired, then we can try to acquire it
                    if (lockValRaw == null || lockVal < ts.TotalSeconds)
                    {
                        ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                        lockExpire = CalculateLockExire(ts, lockTimeout);
                        using (var trans = CreateTransaction())
                        {
                            var expire = lockExpire;
                            trans.QueueCommand(r => ((RedisNativeClient)r).Set(key, Serialize(expire)));
                            if (trans.Commit())
                                wasSet = 1; //acquire lock!
                        }
                    }
                    else
                    {
                        UnWatch();
                    }
                }
                if (wasSet == 1) break;
                System.Threading.Thread.Sleep(sleepIfLockSet);
                totalTime += sleepIfLockSet;
            }
            return (wasSet == 1) ? lockExpire : 0;

        }
        /// <summary>
        /// unlock key
        /// </summary>
        /// <param name="key">global lock key</param>
        /// <param name="setLockValue">value that lock key was set to when it was locked</param>
        public bool Unlock(string key, double setLockValue)
        {
            bool rc = false;
            using (var pipe = CreatePipeline())
            {
                object lockValRaw = null;
                pipe.QueueCommand(r => ((RedisNativeClient) r).Watch(key));
                pipe.QueueCommand(r => ((RedisNativeClient) r).Get(key), x => lockValRaw = Deserialize((x)));
                pipe.Flush();

                var needUnwatch = true;
                if (lockValRaw != null)
                {
                    var lockVal = (double) lockValRaw;
                    if (lockVal == setLockValue)
                    {
                        needUnwatch = false;
                        using (var trans = CreateTransaction())
                        {
                            trans.QueueCommand(r => ((RedisNativeClient) r).Del(key));
                            if (trans.Commit())
                                rc = true;
                        }
                    }
                }
                if (needUnwatch)
                    UnWatch();
            }
            return rc;
        }

        public long FetchGeneration(string generationKey)
        {
            var val = GetValue(generationKey);
            return (val == null) ? Incr(generationKey) : Convert.ToInt64(val);
        }

        public List<byte[]> Serialize(object[] values)
        {
            var rc = new List<byte[]>();
            foreach (var value in values)
            {
                var bytes = Serialize(value);
                if (bytes != null)
                    rc.Add(bytes);
            }
            return rc;
        }



        // Serialize object to buffer
        public  byte[] Serialize(object value)
        {
            if (value == null)
                return null;
             var memoryStream = new MemoryStream();
            memoryStream.Seek(0, 0);
            _bf.Serialize(memoryStream, value);
            return memoryStream.ToArray();
        }

        // Deserialize buffer to object
        public  object Deserialize(byte[] someBytes)
        {         
            if (someBytes == null)
                return null;
            var memoryStream = new MemoryStream();
            memoryStream.Write(someBytes, 0, someBytes.Length);
            memoryStream.Seek(0, 0);
            var de = _bf.Deserialize(memoryStream);
            return de;
        }
        public IList Deserialize(byte[][] byteArray)
        {
            IList rc = new ArrayList();
            foreach (var someBytes in byteArray)
            {
                var obj = Deserialize(someBytes);
                if (obj != null)
                    rc.Add(obj);
            }
            return rc;
        }
    }
}
