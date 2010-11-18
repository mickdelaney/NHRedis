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

        private static readonly string separatorOuter = "#";
        private static readonly string separatorInner = "?";

        //#?#
        private static readonly string regionKeySeparator = separatorOuter + separatorInner + separatorOuter;

        //??
        private static readonly string separatorInnerSanitizer = separatorInner + separatorInner;

        // group names that have only single separatorInner characters in them,
        // and do not end with separatorOuter separatorInner,
        // are valid, reserved group names

        // cache generation - generation changes when cache region is deleted
        private int cacheGeneration=-1;
        
        //sanitized name for cache group (includes prefix and generation)
        private readonly string cacheGroup;
        
        //reserved, unique name for meta entries for this cache group
        private readonly string reservedCacheGroup;

        // key for set of cache group keys
        private readonly string cacheGroupKeys;

        // key for cache group generation
        private readonly string cacheGroupGeneration;

        private readonly string cacheGroupsToClean = separatorInner + "NHIBERNATE_REDIS_CACHE_GROUPS_TO_CLEAN" + separatorInner;

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
            cacheGroup = region;

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
                        cacheGroup = regionPrefix + "_" + region;
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

            cacheGroup = sanitize(cacheGroup);

            //no sanitized string can have an odd-length substring of separatorInner characters
            reservedCacheGroup = separatorInner + cacheGroup;

            cacheGroupKeys = reservedCacheGroup;
            
            //get generation
            cacheGroupGeneration = reservedCacheGroup + "_" + "generation";
            if (clientManager != null)
                cacheGeneration = getGeneration();
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
            IRedisClient client = null;
            try
            {
                client = acquireClient();
                maybeObj = client.Get(globalKey(key));
            }
            catch (Exception)
            {
                log.WarnFormat("could not get: {0}", key);
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
            IRedisClient client = null;
            try
            {
                client = acquireClient();
                client.SetEx(globalKey(key), expiry, bytes);
            }
            catch (Exception)
            {
                log.WarnFormat("could not save: {0} => {1}", key, value);

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
            IRedisClient client = null;
            try
            {
                client = acquireClient();
                client.Del(globalKey(key));
            }
            catch (Exception)
            {
                log.WarnFormat("could not delete key: {0}", key);

            }
            finally
            {
                releaseClient(client);
            }
           
		}

		public void Clear()
		{
            //rename set of keys, and start expiring the keys
            IRedisClient client = null;
            try
            {
                client = acquireClient();
                using (var trans = ((RedisClient)client).CreateTransaction())
                {
                    trans.QueueCommand(r => r.IncrementValue(cacheGroupGeneration));
                    string temp = "temp" + cacheGroupKeys;
                    trans.QueueCommand(r => r.Rename(cacheGroupKeys, temp));
                    initGeneration();
                    trans.QueueCommand(r => r.AddItemToList(cacheGroupsToClean, temp + "," + cacheGeneration.ToString()));
                    trans.Commit();
                    //increment the cache generation
                    cacheGeneration++;
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

        private IRedisClient acquireClient()
        {
            return clientManager.GetClient();
        }
        private void releaseClient(IRedisClient activeClient)
        {
            clientManager.DisposeClient((RedisNativeClient)activeClient);
        }

        private string sanitize(string dirtyString)
        {
            if (dirtyString == null)
                return null;
            return dirtyString.Replace(separatorInner, separatorInnerSanitizer);

        }
        private string sanitize(object dirtyString)
        {
            return sanitize(dirtyString.ToString());
        }
        //group should already be sanitizd
        //key is not expected to be sanitized
        private string globalKey(string group, int generation, object key)
        {
            string rc = sanitize(key);
            if (group != null && !group.Equals(""))
                rc =  group + "_" + generation.ToString() + regionKeySeparator + rc;
            return rc;
        }
        private string globalKey(object key)
        {
            initGeneration();
            return globalKey(cacheGroup, cacheGeneration, key);
        }
        private int getGeneration()
        {
            int rc = 0;
            IRedisClient client = null;
            try
            {
                client = acquireClient();
                string val = client.GetValue(cacheGroupGeneration);
                if (val == null)
                {
                    client.IncrementValue(cacheGroupGeneration);
                }
                else
                {
                    rc = Convert.ToInt32(val);
                }
            }
            finally
            {
                releaseClient(client);
            }
            return rc;
        }
        private void initGeneration()
        {
            if (cacheGeneration == -1)
            {
                cacheGeneration = getGeneration();
            }
        }
	}
}