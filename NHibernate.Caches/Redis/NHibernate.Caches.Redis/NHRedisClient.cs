#region License

//
//  NHRedis - A cache provider for NHibernate using the .NET client
// ServiceStackRedis for Redis
// (http://code.google.com/p/servicestack/wiki/ServiceStackRedis)
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 2.1 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//  Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this library; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// CLOVER:OFF
//

#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using ServiceStack.Redis;
using NHibernate.Cache;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;

using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Redis cache client for Redis.
    /// </summary>
	public class NHRedisClient : ICache
	{
		private static readonly IInternalLogger log;
        private readonly PooledRedisClientManager clientManager;
		private readonly int expiry;

        // NHibernate settings for cache region and prefix
		private readonly string region;
		private readonly string regionPrefix;

        // manage cache region        
        private RedisNamespace cacheNamespace;
 

   		static NHRedisClient()
		{
			log = LoggerProvider.LoggerFor(typeof (RedisClient));
 		}

		public NHRedisClient()
			: this("nhibernate", null)
		{
		}

		public NHRedisClient(string regionName)
			: this(regionName, null)
		{
		}

		public NHRedisClient(string regionName, IDictionary<string, string> properties)
			: this(regionName, properties, null)
		{
		}

        public NHRedisClient(string regionName, IDictionary<string, string> properties, PooledRedisClientManager manager)
		{
			region = regionName;
            string namespacePrefix = region;

            clientManager = manager;
			expiry = 300;

			if (properties != null)
			{
				string expirationString = GetExpirationString(properties);
				if (expirationString != null)
				{
					expiry = Convert.ToInt32(expirationString);
					if (log.IsDebugEnabled)
					{
						log.DebugFormat("using expiration of {0} seconds", expiry);
					}
				}

				if (properties.ContainsKey("regionPrefix"))
				{
					regionPrefix = properties["regionPrefix"];
                    if (regionPrefix != null && !regionPrefix.Equals(""))
                        namespacePrefix = regionPrefix + "_" + region;
					if (log.IsDebugEnabled)
					{
						log.DebugFormat("new regionPrefix :{0}", regionPrefix);
					}
        		}
				else
				{
                   	if (log.IsDebugEnabled)
					{
						log.Debug("no regionPrefix value given, using defaults");
					}
				}
			}
            cacheNamespace = new RedisNamespace(namespacePrefix);

            //make sure generation is synched with server
            synchGeneration();
		}

		#region ICache Members

		public object Get(object key)
		{
			if (key == null)
			{
				return null;
			}
			if (log.IsDebugEnabled)
			{
				log.DebugFormat("fetching object {0} from the cache", key);
			}
            byte[] maybeObj = null;
            IRedisNativeClient client = null;
            try
            {
                client = acquireClient();
                //do transactioned get of generation and value
                //if it succeeds, and null is returned, then either the key doesn't exist or
                // our generation is out of date. In the latter case , update generation and try
                // again.
                int generationFromServer = getGeneration();
                while (true)
                {
                    using (var trans = ((RedisClient)client).CreateTransaction())
                    {
                        trans.QueueCommand(r => r.GetValue(cacheNamespace.getGenerationKey()), x => generationFromServer = Convert.ToInt32(x));
                        trans.QueueCommand(r => ((RedisNativeClient)r).Get(cacheNamespace.globalKey(key)), x => maybeObj = x);
                        trans.Commit();
                    }
                    if (generationFromServer != getGeneration())
                    {
                        //update cached generation value, and try again
                        cacheNamespace.setGeneration(generationFromServer);
                    }
                    else
                        break;
                }

            }
            catch (Exception)
            {
                log.WarnFormat("could not get: {0}", key);
                throw;
            }
            finally
            {
                releaseClient(client);
            }
           
			if (maybeObj == null)
			{
				return null;
			}
            return deSerialize(maybeObj);
		}

		public void Put(object key, object value)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key", "null key not allowed");
			}
			if (value == null)
			{
				throw new ArgumentNullException("value", "null value not allowed");
			}

			if (log.IsDebugEnabled)
			{
				log.DebugFormat("setting value for item {0}", key);
			}
            byte[] bytes = serialize(value);
            IRedisNativeClient client = null;
            try
            {
                client = acquireClient();
                client.SetEx(cacheNamespace.globalKey(key), expiry, bytes);
            }
            catch (Exception)
            {
                log.WarnFormat("could not save: {0} => {1}", key, value);
                throw;

            }
            finally
            {
                releaseClient(client);
            }

       	}

		public void Remove(object key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key");
			}
			if (log.IsDebugEnabled)
			{
				log.DebugFormat("removing item {0}", key);
			}
            IRedisNativeClient client = null;
            try
            {
                client = acquireClient();
                client.Del(cacheNamespace.globalKey(key));
            }
            catch (Exception)
            {
                log.WarnFormat("could not delete key: {0}", key);
                throw;

            }
            finally
            {
                releaseClient(client);
            }
           
		}
        // clear cache region: 
        // 
		public void Clear()
		{
            //rename set of keys, and start expiring the keys
            IRedisNativeClient client = null;
            try
            {
                client = acquireClient();
                using (var trans = ((RedisClient)client).CreateTransaction())
                {               
                     trans.QueueCommand(r => cacheNamespace.setGeneration( r.IncrementValue(cacheNamespace.getGenerationKey()))  );
                     string temp = "temp" + cacheNamespace.getNamespaceKeysKey();
                     trans.QueueCommand(r => ((RedisNativeClient)r).Rename(cacheNamespace.getNamespaceKeysKey(), temp));
                    trans.QueueCommand(r => r.AddItemToList(RedisNamespace.namespacesGarbageKey, temp + "," + getGeneration().ToString()));
                    trans.Commit();
                }
            }
            finally
            {
                releaseClient(client);
            }
		}

		public void Destroy()
		{
			Clear();
		}

		public void Lock(object key)
		{
			// do nothing
		}

		public void Unlock(object key)
		{
			// do nothing
		}

		public long NextTimestamp()
		{
			return Timestamper.Next();
		}

		public int Timeout
		{
			get { return Timestamper.OneMs*60000; }
		}

		public string RegionName
		{
			get { return region; }
		}

		#endregion

        // serialize object to buffer
        private byte[] serialize(object value)
        {
            var dictEntry = new DictionaryEntry(null, value);
            System.IO.MemoryStream memoryStream = new System.IO.MemoryStream(1024);
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(memoryStream, dictEntry);
            return memoryStream.GetBuffer();
        }
        // deSerialize buffer to object
        private object deSerialize(byte[] someBytes)
        {
            System.IO.MemoryStream _memoryStream = new System.IO.MemoryStream(1024);
            BinaryFormatter bf = new BinaryFormatter();
            _memoryStream.Write(someBytes, 0, someBytes.Length);
            _memoryStream.Seek(0, 0);
            DictionaryEntry de = (DictionaryEntry)bf.Deserialize(_memoryStream);
            return de.Value;
        }

        // get value for cache region expiry
		private static string GetExpirationString(IDictionary<string, string> props)
		{
			string result;
			if (!props.TryGetValue("expiration", out result))
			{
				props.TryGetValue(Environment.CacheDefaultExpiration, out result);
			}
			return result;
		}

        // acquire redis client from pool
        private IRedisNativeClient acquireClient()
        {
            if (clientManager == null)
                throw new Exception("acquireClient: clientManager is null");
            return (IRedisNativeClient)clientManager.GetClient();
        }
        // release redis client back to pool
        private void releaseClient(IRedisNativeClient activeClient)
        {
            clientManager.DisposeClient((RedisNativeClient)activeClient);
        }
        // return cache region generation
        private int getGeneration()
        {
            synchGeneration();
            return cacheNamespace.getGeneration();
        }
        // hit server for cache region generation
        private int fetchGeneration()
        {
            int rc = 0;
            IRedisClient client = null;
            try
            {
                client = (IRedisClient)acquireClient();
                string val = client.GetValue(cacheNamespace.getGenerationKey());
                if (val == null)
                {
                    client.Set<int>(cacheNamespace.getGenerationKey(),0);
                }
                else
                {
                    rc = Convert.ToInt32(val);
                }
            }
            finally
            {
                releaseClient((IRedisNativeClient)client);
            }
            return rc;
        }
        // fetch generation value from redis server, if generation is uninitialized
        private void synchGeneration()
        {
            if (cacheNamespace.getGeneration() == -1 && clientManager != null)
            {
                cacheNamespace.setGeneration(fetchGeneration());
            }
        }
	}
}