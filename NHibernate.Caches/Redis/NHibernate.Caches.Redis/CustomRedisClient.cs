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
 

        public CustomRedisClient(string host, int port)
			: base(host, port)
		{
		}

        private double GetLockExire(TimeSpan ts, int timeout)
        {
           return ts.TotalSeconds + timeout + 1;
        }
        
        /// <summary>
        /// distributed, non-reentrant lock
        /// </summary>
        /// <param name="key"></param>
        /// <param name="acquisitionTimeout">timeout for lock acquisition, in seconds</param>
        public double Lock(string key, int acquisitionTimeout, int lockTimeout)
        {
            int totalTime = 0;
            int tryCount = 10;
            int sleepIfLockSet = 500;
            acquisitionTimeout *= 1000;
    
            var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            double lockExpire = GetLockExire(ts, lockTimeout);
            int wasSet = SetNX(key, Serialize(lockExpire));
            while (wasSet == 0 && totalTime < acquisitionTimeout)
            {
                int count = 0;
                while (wasSet == 0 && count < tryCount && totalTime < acquisitionTimeout)
                {
                    System.Threading.Thread.Sleep(sleepIfLockSet);
                    totalTime += sleepIfLockSet;
                    ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                    lockExpire = GetLockExire(ts, lockTimeout);
                    wasSet = SetNX(key, Serialize(lockExpire));
                    count++;
                }
                if (wasSet != 0) break;

                // handle possibliity of crashed client still holding the lock
                var lockVal = Deserialize(Get(key));
                if (lockVal != null && (double)lockVal < ts.TotalSeconds)
                {
                    ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
                    lockExpire = GetLockExire(ts, lockTimeout);
                    lockVal = Deserialize(GetSet(key, Serialize(lockExpire)));
                    // Acquired lock !!
                    if ( (double)lockVal < ts.TotalSeconds)
                        wasSet = 1;
                }
                else
                {
                    System.Threading.Thread.Sleep(sleepIfLockSet);
                    totalTime += sleepIfLockSet;
                }
            }
            return (wasSet == 1) ? lockExpire : 0;

        }

        public void Unlock(string key)
        {
            Del(key);

               
          //  else
          //      Debug.WriteLine(String.Format("tried to unlock key = {0} that was not locked by this client", key));
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
