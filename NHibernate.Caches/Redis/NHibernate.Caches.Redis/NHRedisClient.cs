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
using ServiceStack.Redis.Pipeline;
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
                    var generationFromServer = _cacheNamespace.GetGeneration();
                    while (true)
                    {
                        using (var trans = ((RedisClient) client).CreateTransaction())
                        {
                            trans.QueueCommand(r => ((RedisNativeClient) r).Get(_cacheNamespace.GlobalCacheKey(key)),
                                               x => maybeObj = x);
                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt32(x));
                            trans.Commit();
                        }
                        if (generationFromServer != _cacheNamespace.GetGeneration())
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
                    var generationFromServer = -1;
                    while (true)
                    {
                        using (var trans = client.CreateTransaction())
                        {
                            var globalKey = _cacheNamespace.GlobalCacheKey(key);
                            trans.QueueCommand(r => ((IRedisNativeClient) r).SetEx(globalKey, _expiry, bytes));

                            //add key to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(), globalKey));

                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                                             x => generationFromServer = Convert.ToInt32(x));
                            trans.Commit();
                        }
                        if (generationFromServer != _cacheNamespace.GetGeneration())
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


        private string[] WatchKeys(object key)
        {
            return new[] { _cacheNamespace.GetGenerationKey(), _cacheNamespace.GlobalCacheKey(key) };
 
        }
        
        /// <summary>
        /// Puts a LockedCacheableItem corresponding to (value, version) into
        /// the cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="version"></param>
        /// <param name="versionComparator"></param>
        public void Put(object key, object value, object version, IComparer versionComparator)
        {
            if (key == null)
            {
                return;
            }
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("fetching object {0} from the cache", key);
            }
            byte[] maybeObj = null;
            IRedisPipeline pipe = null;
            IRedisTransaction trans = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    CustomRedisClient client = disposable.Client;

                    long generationFromServer = -1;

                    byte[] bytesToCache = null;

                    pipe = client.CreatePipeline();

                    //watch for changes to generation key and cache key
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(WatchKeys(key)) );

                    pipe.QueueCommand(r => ((RedisNativeClient)r).Get(_cacheNamespace.GlobalCacheKey(key)),
                                       x => maybeObj = x);
                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                        x => generationFromServer = Convert.ToInt32(x));
                    pipe.Flush();

                    //make sure generation is correct before analyzing cache item
                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }

                    // check if can we can put this new (value, version) into the cache
                    LockableCachedItem item = GenerateNewCachedItem(maybeObj, value, version, versionComparator, client);
                    if (item == null)
                        return;
                    bytesToCache = client.Serialize(item);

                    // put new item in cache
                    trans = client.CreateTransaction();

                    trans.QueueCommand(r => ((IRedisNativeClient) r).SetEx(_cacheNamespace.GlobalCacheKey(key),
                                                 _expiry, bytesToCache));

                    //add key to globalKeys set for this namespace
                    trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(), 
                                                 _cacheNamespace.GlobalCacheKey(key)));

                    trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                                 x => generationFromServer = Convert.ToInt32(x));
                    bool success = trans.Commit(); ;
                    while (!success)
                    {
                        pipe.Replay();

                        //make sure generation is correct before analyzing cache item
                        while (generationFromServer != _cacheNamespace.GetGeneration())
                        {
                            //update cached generation value, and try again
                            _cacheNamespace.SetGeneration(generationFromServer);

                            pipe.Replay();
                        }

                        item = GenerateNewCachedItem(maybeObj, value, version, versionComparator, client);
                        if (item == null)
                            return;
                        bytesToCache = client.Serialize(item);
                         success = trans.Replay();
                    }

                    // if we get here, we know that the generation has not been changed
                    // otherwise, the WATCH would have failed the transaction
                    _cacheNamespace.SetGeneration(generationFromServer);
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not get: {0}", key);
                throw;
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
                if (trans != null)
                    trans.Dispose();
            }
        }

      

        /// <summary>
        /// New cache item. Null return indicates that we are not allowed to update the cache, due to versioning
        /// </summary>
        /// <param name="maybeObj"></param>
        /// <param name="value"></param>
        /// <param name="version"></param>
        /// <param name="versionComparator"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        private static LockableCachedItem GenerateNewCachedItem(byte[] maybeObj, object value, object version, IComparer versionComparator, CustomRedisClient client)
        {
            LockableCachedItem newItem = null;
            object currentObject = maybeObj == null ? null : client.Deserialize(maybeObj);
            var currentLockableCachedItem = currentObject as LockableCachedItem;
            // this should never happen....
            if (currentObject != null && currentLockableCachedItem == null)
                throw new NHRedisException();
            if (currentLockableCachedItem == null)
                 newItem = new LockableCachedItem(value, version);
            else if (currentLockableCachedItem.IsPuttable(0, version, versionComparator) )
            {
                currentLockableCachedItem.Update(value, version, versionComparator);
                newItem = currentLockableCachedItem;
            }
            return newItem;
            
        }
         

        /// <summary>
        /// Remove item corresponding to key from cache
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
                    disposable.Client.Del(_cacheNamespace.GlobalCacheKey(key));
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
                        r => r.IncrementValue(_cacheNamespace.GetGenerationKey()), x =>  _cacheNamespace.SetGeneration(x) );
                    var temp = "temp_" + _cacheNamespace.GetGlobalKeysKey() + "_" + _cacheNamespace.GetGeneration();
                    trans.QueueCommand(r => ((RedisNativeClient) r).Rename(_cacheNamespace.GetGlobalKeysKey(), temp), null, e => Log.Debug(e) );
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
                    disposable.Client.Lock(_cacheNamespace.GlobalKey(key,RedisNamespace.NumTagsForLockKey));
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
                    disposable.Client.Unlock(_cacheNamespace.GlobalKey(key,RedisNamespace.NumTagsForLockKey));
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

        /// <summary>
        /// Return a dictionary of (key,value) pairs corresponding to a collection of keys
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public IDictionary MultiGet(IEnumerable keys)
        {
            var rc = new Dictionary<object,object>();
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var globalKeys = new List<string>();
                //generate global keys
                int keyCount = 0;
                // Note: should get generation
                foreach (var key in keys)
                {
                    keyCount++;
                    globalKeys.Add(_cacheNamespace.GlobalCacheKey(key));
                }
                // do multi get
                var resultBytesArray = client.MGet(globalKeys.ToArray());
                if (keyCount != resultBytesArray.Length)
                    throw new RedisException("Prefetch: number of results does not match number of keys");

                //process results
                IEnumerator iter = keys.GetEnumerator();   
                iter.MoveNext();
                foreach (byte[] resultBytes in resultBytesArray)
                {
                    if (resultBytes != null)
                    {
                        var currentObject = client.Deserialize(resultBytes);
                        if (currentObject != null)
                            rc[iter.Current] = currentObject;
                    }
                    iter.MoveNext();
                }
            }
            return rc;
        }

        public IList SMembers(object key)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var members = disposable.Client.SMembers(_cacheNamespace.GlobalCacheKey(key));
                IList rc = new ArrayList();
                foreach(byte[] item in members)
                {
                    rc.Add(disposable.Client.Deserialize(item));
                }
                return rc;
            }
            
        }
        public bool SAdd(object key, object value)
        {
            int rc = 0;
            using (var disposable = new DisposableClient(_clientManager))
            {
                byte[] bytes = disposable.Client.Serialize(value);
                rc = disposable.Client.SAdd(_cacheNamespace.GlobalCacheKey(key),bytes);
            }
            return rc == 1;
        }
 
        public bool SAdd(object key, IList keys)
        {
            bool success = true;
            using (var disposable = new DisposableClient(_clientManager))
            {
                CustomRedisClient client = disposable.Client;
                using (var pipe = client.CreatePipeline())
                {
                    foreach (var k in keys)
                    {
                        byte[] bytes = client.Serialize(k);
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SAdd(_cacheNamespace.GlobalCacheKey(key), bytes), x => success &= x == 1  );
                    }
                    success = true;
                    pipe.Flush();
                }
            }
            return success;
        }

        public bool SRemove(object key, object value)
        {
            int rc = 0;
            using (var disposable = new DisposableClient(_clientManager))
            {
                byte[] bytes = disposable.Client.Serialize(value);
                rc = disposable.Client.SRem(_cacheNamespace.GlobalCacheKey(key), bytes);
            }
            return rc == 1;
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
        /// hit server for cache _region generation
        /// </summary>
        /// <returns></returns>
        private int FetchGeneration()
        {
            int rc = 0;
            using (var disposable = new DisposableClient(_clientManager))
            {
                rc = disposable.Client.FetchGeneration(_cacheNamespace.GetGenerationKey());
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