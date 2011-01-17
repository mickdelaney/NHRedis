using System.Collections.Generic;
using System.Diagnostics;
using Iesi.Collections;
using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections;
using System.IO;
using System;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClient : RedisClient
    {
        private BinaryFormatter _bf = new BinaryFormatter();
        private HashSet<object> acquiredLocks = new HashSet<object>();

        public CustomRedisClient(string host, int port)
			: base(host, port)
		{
		}

        public void Lock(string lockKey)
        {
            if (acquiredLocks.Contains(lockKey))
                return;

            int totalTime = 0;
            int lockTimeout = 60;
            int timeout = 60;
            int tryCount = 10;
            int sleepIfLockSet = 500;
    
         
            var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            int wasSet = SetNX(lockKey, Serialize(ts.TotalSeconds + lockTimeout + 1));
            while (wasSet == 0 && totalTime < timeout)
            {
                int count = 0;
                while (wasSet == 0 && count < tryCount)
                {
                    System.Threading.Thread.Sleep(sleepIfLockSet);
                    totalTime += sleepIfLockSet;
                    ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                    wasSet = SetNX(lockKey, Serialize(ts.TotalSeconds + lockTimeout + 1));
                    count++;
                }
                // handle possibliity of crashed client still holding the lock
                if (wasSet == 0)
                {
                    var lockVal = (double) Deserialize(Get(lockKey));
                    if (lockVal < ts.TotalSeconds)
                    {
                        ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                        lockVal = (double) Deserialize(GetSet(lockKey, Serialize(ts.TotalSeconds + lockTimeout + 1)));
                        // Acquired lock !!
                        if (lockVal < ts.TotalSeconds)
                            wasSet = 1;
                    }
                }

            }
            if (wasSet == 1)
                acquiredLocks.Add(lockKey);
        }

        public void Unlock(string lockKey)
        {
            if (acquiredLocks.Contains(lockKey))
                Del(lockKey);
            else
                Debug.WriteLine(String.Format("tried to unlock key = {0} that was not locked by this client", lockKey));
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
             MemoryStream _memoryStream = new MemoryStream();
            _memoryStream.Seek(0, 0);
            _bf.Serialize(_memoryStream, value);
            return _memoryStream.ToArray();
        }

        // Deserialize buffer to object
        public  object Deserialize(byte[] someBytes)
        {         
            if (someBytes == null)
                return null;
            MemoryStream _memoryStream = new MemoryStream();
            _memoryStream.Write(someBytes, 0, someBytes.Length);
            _memoryStream.Seek(0, 0);
            var de = _bf.Deserialize(_memoryStream);
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
