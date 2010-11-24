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
	public class NhRedisClient : ICache
	{
		private static readonly IInternalLogger Log;
        private readonly PooledRedisClientManager _clientManager;
		private readonly int _expiry;

        // NHibernate settings for cache _region and prefix
		private readonly string _region;
		private readonly string _regionPrefix;

        // manage cache _region        
        private readonly RedisNamespace cacheNamespace;
 

   		static NhRedisClient()
		{
			Log = LoggerProvider.LoggerFor(typeof (RedisClient));
 		}

		public NhRedisClient()
			: this("nhibernate", null)
		{
		}

		public NhRedisClient(string regionName)
			: this(regionName, null)
		{
		}

		public NhRedisClient(string regionName, IDictionary<string, string> properties)
			: this(regionName, properties, null)
		{
		}

        public NhRedisClient(string regionName, IDictionary<string, string> properties, PooledRedisClientManager manager)
		{
			_region = regionName;
            var namespacePrefix = _region;

            _clientManager = manager;
			_expiry = 300;

			if (properties != null)
			{
				string expirationString = GetExpirationString(properties);
				if (expirationString != null)
				{
					_expiry = Convert.ToInt32(expirationString);
					if (Log.IsDebugEnabled)
					{
						Log.DebugFormat("using expiration of {0} seconds", _expiry);
					}
				}

				if (properties.ContainsKey("_regionPrefix"))
				{
					_regionPrefix = properties["_regionPrefix"];
                    if (_regionPrefix != null && !_regionPrefix.Equals(""))
                        namespacePrefix = _regionPrefix + "_" + _region;
					if (Log.IsDebugEnabled)
					{
						Log.DebugFormat("new _regionPrefix :{0}", _regionPrefix);
					}
        		}
				else
				{
                   	if (Log.IsDebugEnabled)
					{
						Log.Debug("no _regionPrefix value given, using defaults");
					}
				}
			}
            cacheNamespace = new RedisNamespace(namespacePrefix);

            //make sure generation is synched with server
            SynchGeneration();
		}

		#region ICache Members

		public object Get(object key)
		{
			if (key == null)
			{
				return null;
			}
			if (Log.IsDebugEnabled)
			{
				Log.DebugFormat("fetching object {0} from the cache", key);
			}
            byte[] maybeObj = null;
            IRedisNativeClient client = null;
            try
            {
                client = AcquireClient();
                //do transactioned get of generation and value
                //if it succeeds, and null is returned, then either the key doesn't exist or
                // our generation is out of date. In the latter case , update generation and try
                // again.
                int generationFromServer = GetGeneration();
                while (true)
                {
                    using (var trans = ((RedisClient)client).CreateTransaction())
                    {
                        trans.QueueCommand(r => r.GetValue(cacheNamespace.GetGenerationKey()), x => generationFromServer = Convert.ToInt32(x));
                        trans.QueueCommand(r => ((RedisNativeClient)r).Get(cacheNamespace.GlobalKey(key)), x => maybeObj = x);
                        trans.Commit();
                    }
                    if (generationFromServer != GetGeneration())
                    {
                        //update cached generation value, and try again
                        cacheNamespace.SetGeneration(generationFromServer);
                    }
                    else
                        break;
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
            finally
            {
                ReleaseClient(client);
            }
           
			if (maybeObj == null)
			{
				return null;
			}
            return DeSerialize(maybeObj);
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

			if (Log.IsDebugEnabled)
			{
				Log.DebugFormat("setting value for item {0}", key);
			}
            byte[] bytes = Serialize(value);
            IRedisNativeClient client = null;
            try
            {
                // should this be in a transaction??
                client = AcquireClient();
                string globalKey = cacheNamespace.GlobalKey(key);
                //add global (key,value)
                client.SetEx(globalKey, _expiry, bytes);
                //add key to globalKeys set for this namespace
                ((IRedisClient)client).AddItemToSet(cacheNamespace.GetGlobalKeysKey(), globalKey); 
            }
            catch (Exception)
            {
                Log.WarnFormat("could not save: {0} => {1}", key, value);
                throw;

            }
            finally
            {
                ReleaseClient(client);
            }

       	}

		public void Remove(object key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key");
			}
			if (Log.IsDebugEnabled)
			{
				Log.DebugFormat("removing item {0}", key);
			}
            IRedisNativeClient client = null;
            try
            {
                client = AcquireClient();
                client.Del(cacheNamespace.GlobalKey(key));
            }
            catch (Exception)
            {
                Log.WarnFormat("could not delete key: {0}", key);
                throw;

            }
            finally
            {
                ReleaseClient(client);
            }
           
		}
        // clear cache _region: 
        // 
		public void Clear()
		{
            //rename set of keys, and Start expiring the keys
            IRedisNativeClient client = null;
            try
            {
                client = AcquireClient();
                using (var trans = ((RedisClient)client).CreateTransaction())
                {               
                     trans.QueueCommand(r => cacheNamespace.SetGeneration( r.IncrementValue(cacheNamespace.GetGenerationKey()))  );
                     string temp = "temp_" + cacheNamespace.GetGlobalKeysKey() + "_" + GetGeneration().ToString();
                     trans.QueueCommand(r => ((RedisNativeClient)r).Rename(cacheNamespace.GetGlobalKeysKey(), temp));
                    trans.QueueCommand(r => r.AddItemToList(RedisNamespace.NamespacesGarbageKey, temp));
                    trans.Commit();
                }
            }
            finally
            {
                ReleaseClient(client);
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
			get { return _region; }
		}

		#endregion

        // Serialize object to buffer
        private static byte[] Serialize(object value)
        {
            var dictEntry = new DictionaryEntry(null, value);
            var memoryStream = new System.IO.MemoryStream(1024);
            var bf = new BinaryFormatter();
            bf.Serialize(memoryStream, dictEntry);
            return memoryStream.GetBuffer();
        }
        // DeSerialize buffer to object
        private static object DeSerialize(byte[] someBytes)
        {
            var memoryStream = new System.IO.MemoryStream(1024);
            var bf = new BinaryFormatter();
            memoryStream.Write(someBytes, 0, someBytes.Length);
            memoryStream.Seek(0, 0);
            var de = (DictionaryEntry)bf.Deserialize(memoryStream);
            return de.Value;
        }

        // get value for cache _region _expiry
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
        private IRedisNativeClient AcquireClient()
        {
            if (_clientManager == null)
                throw new Exception("AcquireClient: _clientManager is null");
            return (IRedisNativeClient)_clientManager.GetClient();
        }
        // release redis client back to pool
        private void ReleaseClient(IRedisNativeClient activeClient)
        {
            _clientManager.DisposeClient((RedisNativeClient)activeClient);
        }
        // return cache _region generation
        private int GetGeneration()
        {
            SynchGeneration();
            return cacheNamespace.GetGeneration();
        }
        // hit server for cache _region generation
        private int FetchGeneration()
        {
            int rc = 0;
            IRedisClient client = null;
            try
            {
                client = (IRedisClient)AcquireClient();
                string val = client.GetValue(cacheNamespace.GetGenerationKey());
                if (val == null)
                {
                    client.Set<int>(cacheNamespace.GetGenerationKey(),0);
                }
                else
                {
                    rc = Convert.ToInt32(val);
                }
            }
            finally
            {
                ReleaseClient((IRedisNativeClient)client);
            }
            return rc;
        }
        // fetch generation value from redis server, if generation is uninitialized
        private void SynchGeneration()
        {
            if (cacheNamespace.GetGeneration() == -1 && _clientManager != null)
            {
                cacheNamespace.SetGeneration(FetchGeneration());
            }
        }
	}
}