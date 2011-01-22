using System.Collections.Generic;
using ServiceStack.Redis;
using System.Collections;
using System;
using ServiceStack.Redis.Support;
using ServiceStack.Redis.Support.Locking;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClient : RedisClient
    {
        private readonly DistributedLock _lock = new DistributedLock();
        private readonly ObjectSerializer _serializer = new ObjectSerializer();
   
        public CustomRedisClient(string host, int port)
			: base(host, port)
		{
		}
        
        /// <summary>
        /// acquire distributed, non-reentrant lock on key
        /// </summary>
        /// <param name="key">global key for this lock</param>
        /// <param name="acquisitionTimeout">timeout for acquiring lock</param>
        /// <param name="lockTimeout">timeout for lock, in seconds (stored as value against lock key) </param>
        public double Lock(string key, int acquisitionTimeout, int lockTimeout)
        {
            return _lock.Lock(this, key, acquisitionTimeout, lockTimeout);

        }
        /// <summary>
        /// unlock key
        /// </summary>
        /// <param name="key">global lock key</param>
        /// <param name="setLockValue">value that lock key was set to when it was locked</param>
        public bool Unlock(string key, double setLockValue)
        {
            return _lock.Unlock(this, key, setLockValue);
        }

        /// <summary>
        /// fetch generation (for cache region)
        /// </summary>
        /// <param name="generationKey"></param>
        /// <returns></returns>
        public long FetchGeneration(string generationKey)
        {
            var val = GetValue(generationKey);
            return (val == null) ? Incr(generationKey) : Convert.ToInt64(val);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values">array of serializable objects</param>
        /// <returns></returns>
        public List<byte[]> Serialize(object[] values)
        {
            return _serializer.Serialize(values);
        }

        /// <summary>
        ///  Serialize object to buffer
        /// </summary>
        /// <param name="value">serializable object</param>
        /// <returns></returns>
        public  byte[] Serialize(object value)
        {
            return _serializer.Serialize(value);
        }

        /// <summary>
        ///     Deserialize buffer to object
        /// </summary>
        /// <param name="someBytes">byte array to deserialize</param>
        /// <returns></returns>
        public  object Deserialize(byte[] someBytes)
        {
            return _serializer.Deserialize(someBytes);
        }
        /// <summary>
        /// deserialize an array of byte arrays
        /// </summary>
        /// <param name="byteArray"></param>
        /// <returns></returns>
        public IList Deserialize(byte[][] byteArray)
        {
            return _serializer.Deserialize(byteArray);
        }
    }
}
