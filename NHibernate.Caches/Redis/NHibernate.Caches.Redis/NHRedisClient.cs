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
	public class NHRedisClient : ICache
	{
		private static readonly IInternalLogger log;
        private readonly PooledRedisClientManager clientManager;
		private readonly int expiry;

        // NHibernate settings for cache region and prefix
		private readonly string region;
		private readonly string regionPrefix;

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
            if (clientManager != null)
                cacheNamespace.setGeneration(getGeneration());
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
                maybeObj = client.Get(globalKey(key));
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
                client.SetEx(globalKey(key), expiry, bytes);
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
                client.Del(globalKey(key));
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

		public void Clear()
		{
            //rename set of keys, and start expiring the keys
            IRedisNativeClient client = null;
            try
            {
                client = acquireClient();
                initGeneration();
                using (var trans = ((RedisClient)client).CreateTransaction())
                {
                     trans.QueueCommand(r => r.IncrementValue(cacheNamespace.getGenerationKey()));
                     string temp = "temp" + cacheNamespace.getNamespaceKeysKey();
                     trans.QueueCommand(r => r.Rename(cacheNamespace.getNamespaceKeysKey(), temp));
                    trans.QueueCommand(r => r.AddItemToList(RedisNamespace.namespacesGarbageKey, temp + "," + cacheNamespace.getGeneration().ToString()));
                    trans.Commit();

                    //increment the local value of the cache generation
                    cacheNamespace.incrementGeneration();
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

        private byte[] serialize(object value)
        {
            var dictEntry = new DictionaryEntry(null, value);
            System.IO.MemoryStream memoryStream = new System.IO.MemoryStream(1024);
            BinaryFormatter bf = new BinaryFormatter();
            bf.Serialize(memoryStream, dictEntry);
            return memoryStream.GetBuffer();
        }

        private object deSerialize(byte[] someBytes)
        {
            System.IO.MemoryStream _memoryStream = new System.IO.MemoryStream(1024);
            BinaryFormatter bf = new BinaryFormatter();
            _memoryStream.Write(someBytes, 0, someBytes.Length);
            _memoryStream.Seek(0, 0);
            DictionaryEntry de = (DictionaryEntry)bf.Deserialize(_memoryStream);
            return de.Value;
        }

		private static string GetExpirationString(IDictionary<string, string> props)
		{
			string result;
			if (!props.TryGetValue("expiration", out result))
			{
				props.TryGetValue(Environment.CacheDefaultExpiration, out result);
			}
			return result;
		}

        private IRedisNativeClient acquireClient()
        {
            if (clientManager == null)
                throw new Exception("acquireClient: clientManager is null");
            return (IRedisNativeClient)clientManager.GetClient();
        }
        private void releaseClient(IRedisNativeClient activeClient)
        {
            clientManager.DisposeClient((RedisNativeClient)activeClient);
        }

        private string globalKey(object key)
        {
            initGeneration();
            return cacheNamespace.globalKey(key);
        }
        private int getGeneration()
        {
            int rc = 0;
            IRedisClient client = null;
            try
            {
                client = (IRedisClient)acquireClient();
                string val = client.GetValue(cacheNamespace.getGenerationKey());
                if (val == null)
                {
                    client.IncrementValue(cacheNamespace.getGenerationKey());
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
        private void initGeneration()
        {
            if (cacheNamespace.getGeneration() == -1)
            {
                cacheNamespace.setGeneration(getGeneration());
            }
        }
	}
}