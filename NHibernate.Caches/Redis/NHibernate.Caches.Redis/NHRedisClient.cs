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
using System.Collections.Generic;
using ServiceStack.Redis;
using NHibernate.Cache;
using System.Threading;
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
            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
                //do transactioned get of generation and value
                //if it succeeds, and null is returned, then either the key doesn't exist or
                // our generation is out of date. In the latter case , update generation and try
                // again.
                var generationFromServer = GetGeneration();
                while (true)
                {
                    using (var trans = ((RedisClient)client).CreateTransaction())
                    {
                        trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()), x => generationFromServer = Convert.ToInt32(x));
                        trans.QueueCommand(r => ((RedisNativeClient)r).Get(_cacheNamespace.GlobalKey(key)), x => maybeObj = x);
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
                rc =  maybeObj == null ? null : client.Deserialize(maybeObj);

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

		    return rc;
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

            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
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
                        var globalKey = _cacheNamespace.GlobalKey(key);
                        trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()), x => generationFromServer = Convert.ToInt32(x));
                        trans.QueueCommand(r => ((IRedisNativeClient)r).SetEx(globalKey,_expiry, bytes));

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
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
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
            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
                client.Del(_cacheNamespace.GlobalKey(key));
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
            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
                using (var trans = client.CreateTransaction())
                {               
                     trans.QueueCommand(r => _cacheNamespace.SetGeneration( r.IncrementValue(_cacheNamespace.GetGenerationKey()))  );
                     var temp = "temp_" + _cacheNamespace.GetGlobalKeysKey() + "_" + GetGeneration().ToString();
                     trans.QueueCommand(r => ((RedisNativeClient)r).Rename(_cacheNamespace.GetGlobalKeysKey(), temp));
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
            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
                client.Lock(_cacheNamespace.GlobalLockKey(key));              
            }
            catch (Exception)
            {
                Log.WarnFormat("could not acquire lock for key: {0}", key);
                throw;

            }
            finally
            {
                ReleaseClient(client);
            }
		}

		public void Unlock(object key)
		{
            CustomRedisClient client = null;
            var temp = new byte[1];
            temp[0] = 1;
            try
            {
                client = AcquireClient();
                client.Unlock(_cacheNamespace.GlobalLockKey(key));
              
            }
            catch (Exception)
            {
                Log.WarnFormat("could not release lock for key: {0}", key);
                throw;

            }
            finally
            {
                ReleaseClient(client);
            }
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
        private CustomRedisClient AcquireClient()
        {
            if (_clientManager == null)
                throw new Exception("AcquireClient: _clientManager is null");
            return (CustomRedisClient)_clientManager.GetClient();
        }
        // release redis client back to pool
        private void ReleaseClient(RedisNativeClient activeClient)
        {
            _clientManager.DisposeClient(activeClient);
        }
        // return cache _region generation
        private int GetGeneration()
        {
            SynchGeneration();
            return _cacheNamespace.GetGeneration();
        }
        // hit server for cache _region generation
        private int FetchGeneration()
        {
            int rc = 0;
            CustomRedisClient client = null;
            try
            {
                client = AcquireClient();
                rc = client.FetchGeneration(_cacheNamespace.GetGenerationKey());
            }
            finally
            {
                ReleaseClient(client);
            }
            return rc;
        }
        // fetch generation value from redis server, if generation is uninitialized
        private void SynchGeneration()
        {
            if (_cacheNamespace.GetGeneration() == -1 && _clientManager != null)
            {
                _cacheNamespace.SetGeneration(FetchGeneration());
            }
        }
	}
}