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
using ServiceStack.Redis;
using NHibernate.Cache;
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
        private readonly RedisNamespace _cacheNamespace;
 

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
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClient(string regionName, IDictionary<string, string> properties, PooledRedisClientManager manager)
		{
			_region = regionName;
            var namespacePrefix = _region;

            _clientManager = manager;
			_expiry = 300;

			if (properties != null)
			{
				var expirationString = GetExpirationString(properties);
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
            _cacheNamespace = new RedisNamespace(namespacePrefix);

            //make sure generation is synched with server
            SynchGeneration();
		}

		#region ICache Members
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
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
		    object rc = null;

            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    var generationFromServer = GetGeneration();
                    while (true)
                    {
                        using (var trans = ((RedisClient) client).CreateTransaction())
                        {
                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt32(x));
                            trans.QueueCommand(r => ((RedisNativeClient) r).Get(_cacheNamespace.GlobalKey(key, 0)),
                                               x => maybeObj = x);
                            trans.Commit();
                        }
                        if (generationFromServer != GetGeneration())
                        {
                            //update cached generation value, and try again
                            _cacheNamespace.SetGeneration(generationFromServer);
                        }
                        else
                            break;
                    }
                    rc = maybeObj == null ? null : client.Deserialize(maybeObj);
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
		    return rc;
		}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public object Get(long sessionId, object key)
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
            object rc = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    var generationFromServer = GetGeneration();
                    while (true)
                    {
                        using (var trans = ((RedisClient) client).CreateTransaction())
                        {
                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt32(x));
                            trans.QueueCommand(r => ((RedisNativeClient) r).Get(_cacheNamespace.GlobalKey(key, 0)),
                                               x => maybeObj = x);
                            trans.Commit();
                        }
                        if (generationFromServer != GetGeneration())
                        {
                            //update cached generation value, and try again
                            _cacheNamespace.SetGeneration(generationFromServer);
                        }
                        else
                            break;
                    }
                    rc = maybeObj == null ? null : client.Deserialize(maybeObj);
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
           

            return rc;
        
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
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

            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    var bytes = client.Serialize(value);
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    var generationFromServer = GetGeneration();
                    while (true)
                    {
                        using (var trans = client.CreateTransaction())
                        {
                            var globalKey = _cacheNamespace.GlobalKey(key, 0);
                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt32(x));
                            trans.QueueCommand(r => ((IRedisNativeClient) r).SetEx(globalKey, _expiry, bytes));

                            //add key to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(), globalKey));
                            trans.Commit();
                        }
                        if (generationFromServer != GetGeneration())
                        {
                            //update cached generation value, and try again
                            _cacheNamespace.SetGeneration(generationFromServer);
                        }
                        else
                            break;
                    }
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
       	}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="version"></param>
        /// <param name="versionComparator"></param>
        public void Put(long sessionId, object key, object value, object version, IComparer versionComparator)
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

            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    var bytes = client.Serialize(value);
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    var generationFromServer = GetGeneration();
                    while (true)
                    {
                        using (var trans = client.CreateTransaction())
                        {
                            var globalKey = _cacheNamespace.GlobalKey(key, 0);
                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt32(x));
                            trans.QueueCommand(r => ((IRedisNativeClient) r).SetEx(globalKey, _expiry, bytes));

                            //add key to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(), globalKey));
                            trans.Commit();
                        }
                        if (generationFromServer != GetGeneration())
                        {
                            //update cached generation value, and try again
                            _cacheNamespace.SetGeneration(generationFromServer);
                        }
                        else
                            break;
                    }
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
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
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    client.Del(_cacheNamespace.GlobalKey(key, 0));
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not delete key: {0}", key);
                throw;

            }           
		}
 
        /// <summary>
        /// clear cache region
        /// </summary>
		public void Clear()
		{
            //rename set of keys, and Start expiring the keys

            using (var disposable = new DisposableClient(_clientManager))
            {
                CustomRedisClient client = disposable.Client;
                using (var trans = client.CreateTransaction())
                {
                    trans.QueueCommand(
                        r => _cacheNamespace.SetGeneration(r.IncrementValue(_cacheNamespace.GetGenerationKey())));
                    var temp = "temp_" + _cacheNamespace.GetGlobalKeysKey() + "_" + GetGeneration().ToString();
                    trans.QueueCommand(r => ((RedisNativeClient) r).Rename(_cacheNamespace.GetGlobalKeysKey(), temp));
                    trans.QueueCommand(r => r.AddItemToList(RedisNamespace.NamespacesGarbageKey, temp));
                    trans.Commit();
                }
            }

		}
        /// <summary>
        /// 
        /// </summary>
		public void Destroy()
		{
			Clear();
		}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
		public void Lock(object key)
		{
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    client.Lock(_cacheNamespace.GlobalLockKey(key));
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not acquire lock for key: {0}", key);
                throw;

            }
		}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
		public void Unlock(object key)
		{
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;
                    client.Unlock(_cacheNamespace.GlobalLockKey(key));
                }

            }
            catch (Exception)
            {
                Log.WarnFormat("could not release lock for key: {0}", key);
                throw;

            }
		}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="key"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        public long LockCow(long sessionId, object key)
        {
 
            //1. if key is not in cow store, then create item (with null version and value)
             // (lock count set to one in constructor)

            //3. else increment lock on existing item
            return 0;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sessionId"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public long UnlockCow(long sessionId, object key, bool writeBack)
        {
  
            //1. decrement lock count on item

            //2. if lock count is at zero, and writeBack is true, then update backing store, 
            // and delete item from cow store

            return 0;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDisposable GetReadLock()
        {
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDisposable GetWriteLock()
        {
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long NextTimestamp()
		{
			return Timestamper.Next();
		}
        /// <summary>
        /// 
        /// </summary>
		public int Timeout
		{
			get { return Timestamper.OneMs*60000; }
		}
        /// <summary>
        /// 
        /// </summary>
		public string RegionName
		{
			get { return _region; }
		}

		#endregion


        /// <summary>
        /// get value for cache _region _expiry
        /// </summary>
        /// <param name="props"></param>
        /// <returns></returns>
		private static string GetExpirationString(IDictionary<string, string> props)
		{
			string result;
			if (!props.TryGetValue("expiration", out result))
			{
				props.TryGetValue(Environment.CacheDefaultExpiration, out result);
			}
			return result;
		}



        /// <summary>
        /// return cache _region generation
        /// </summary>
        /// <returns></returns>
        private long GetGeneration()
        {
            SynchGeneration();
            return _cacheNamespace.GetGeneration();
        }
 
        /// <summary>
        /// hit server for cache _region generation
        /// </summary>
        /// <returns></returns>
        private int FetchGeneration()
        {
            int rc = 0;
            using (var disposable = new DisposableClient(_clientManager))
            {
                CustomRedisClient client = disposable.Client;
                rc = client.FetchGeneration(_cacheNamespace.GetGenerationKey());
            }
            return rc;
        }

        /// <summary>
        /// fetch generation value from redis server, if generation is uninitialized 
        /// </summary>
        private void SynchGeneration()
        {
            if (_cacheNamespace.GetGeneration() == -1 && _clientManager != null)
            {
                _cacheNamespace.SetGeneration(FetchGeneration());
            }
        }
	}
}