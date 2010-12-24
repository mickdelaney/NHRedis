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
        protected readonly PooledRedisClientManager _clientManager;
        protected readonly int _expiry;

        // NHibernate settings for cache _region and prefix
        protected readonly string _region;
        protected readonly string _regionPrefix;

        // manage cache _region        
        protected readonly RedisNamespace _cacheNamespace;

   		static NhRedisClient()
		{
            Log = LoggerProvider.LoggerFor(typeof(NhRedisClient));
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
		public virtual object Get(object key)
		{
			if (key == null)
				return null;
			if (Log.IsDebugEnabled)
				Log.DebugFormat("fetching object {0} from the cache", key);

            byte[] maybeObj = null;
		    object rc;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();
                  
                    pipe.QueueCommand(r => ((RedisNativeClient) r).Get(_cacheNamespace.GlobalCacheKey(key)),
                                       x => maybeObj = x);
                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                       x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();
                   
                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
      
                    rc = client.Deserialize(maybeObj);
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
            }
		    return rc;
		}
      
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public virtual void Put(object key, object value)
		{
			if (key == null)
				throw new ArgumentNullException("key", "null key not allowed");
			if (value == null)
				throw new ArgumentNullException("value", "null value not allowed");
			if (Log.IsDebugEnabled)
				Log.DebugFormat("setting value for item {0}", key);

            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    var bytes = client.Serialize(value);
                    //do transactioned get of generation and value
                    //if it succeeds, and null is returned, then either the key doesn't exist or
                    // our generation is out of date. In the latter case , update generation and try
                    // again.
                    long generationFromServer = -1;
                    while (true)
                    {
                        using (var trans = client.CreateTransaction())
                        {
                            trans.QueueCommand(r => ((IRedisNativeClient)r).SetEx(_cacheNamespace.GlobalCacheKey(key),
                                                                _expiry, bytes));

                            //add key to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(),
                                                                _cacheNamespace.GlobalCacheKey(key)));

                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                                             x => generationFromServer = Convert.ToInt64(x));
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

        protected string[] GlobalKeys(IEnumerable<ScratchCacheItem> scratchItems, bool includeGenerationKey)
        {
            var nonNull = new List<string>();
            if (includeGenerationKey)
                nonNull.Add(_cacheNamespace.GetGenerationKey());
            foreach( var item in scratchItems)
            {
                if (item.PutParameters.Key != null)
                    nonNull.Add(_cacheNamespace.GlobalCacheKey(item.PutParameters.Key));
            }
            return nonNull.ToArray();
        }

      
   
        /// <summary>
        /// Puts a LockedCacheableItem corresponding to (value, version) into
        /// the cache
        /// </summary>
        /// <param name="putParameters"></param>
        public virtual void Put(IList<VersionedPutParameters> putParameters)
        {
            //deal with null keys
            IList<ScratchCacheItem> scratchItems = new List<ScratchCacheItem>();
            foreach (var putParams in putParameters)
            {
                if (putParams.Key == null) continue;
                scratchItems.Add(new ScratchCacheItem(putParams));
                if (Log.IsDebugEnabled)
                    Log.DebugFormat("fetching object {0} from the cache", putParams.Key.ToString());
            }
            if (scratchItems.Count == 0)  return;

            byte[][] currentItemsRaw = null;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;

                    long generationFromServer = -1;

                    pipe = client.CreatePipeline();

                    //watch for changes to generation key and cache key
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(GlobalKeys(scratchItems, true)));

                    //get all of the current objects
                    pipe.QueueCommand(r => ((RedisNativeClient)r).MGet(GlobalKeys(scratchItems, false)), x => currentItemsRaw = x);

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()), x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    //make sure generation is correct before analyzing cache item
                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }

                    // check if there is a new value to put
                    scratchItems = GenerateNewCacheItems(currentItemsRaw, scratchItems, client);
                    if (scratchItems.Count == 0)
                        return;

                    bool success;

                    // put new item in cache
                    using (var trans = client.CreateTransaction())
                    {
                        foreach (var scratch in scratchItems)
                        {
                            //setex on all new objects
                            trans.QueueCommand(
                                r =>
                                ((IRedisNativeClient) r).SetEx(
                                    _cacheNamespace.GlobalCacheKey(scratch.PutParameters.Key),
                                    _expiry, scratch.NewCacheItemRaw));

                            //add keys to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(),
                                                                   _cacheNamespace.GlobalCacheKey(
                                                                       scratch.PutParameters.Key)));
                        }

                        trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                           x => generationFromServer = Convert.ToInt64(x));
                        success = trans.Commit();
                    }
                    
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

                        // check if there is a new value to put
                        scratchItems = GenerateNewCacheItems(currentItemsRaw, scratchItems, client);
                        if (scratchItems.Count == 0)
                            return;

                        // put new item in cache
                        using (var trans = client.CreateTransaction())
                        {
                            foreach (var scratch in scratchItems)
                            {
                                //setex on all new objects
                                trans.QueueCommand(
                                    r =>
                                    ((IRedisNativeClient)r).SetEx(
                                        _cacheNamespace.GlobalCacheKey(scratch.PutParameters.Key),
                                        _expiry, scratch.NewCacheItemRaw));

                                //add keys to globalKeys set for this namespace
                                trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(),
                                                                       _cacheNamespace.GlobalCacheKey(
                                                                           scratch.PutParameters.Key)));
                            }

                            trans.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                               x => generationFromServer = Convert.ToInt64(x));
                            success = trans.Commit();
                        }
                    }

                    // if we get here, we know that the generation has not been changed
                    // otherwise, the WATCH would have failed the transaction
                    _cacheNamespace.SetGeneration(generationFromServer);
                }
            }
            catch (Exception)
            {
                foreach (var putParams in putParameters)
                {
                    Log.WarnFormat("could not get: {0}", putParams.Key);
                }
                
                throw;
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
        }

    
        /// <summary>
        /// New cache item. Null return indicates that we are not allowed to update the cache, due to versioning
        /// </summary>
        /// <param name="currentItemsRaw"></param>
        /// <param name="scratchItems"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        protected static IList<ScratchCacheItem> GenerateNewCacheItems(byte[][] currentItemsRaw, IList<ScratchCacheItem> scratchItems, CustomRedisClient client)
        {
            if (currentItemsRaw.Length != scratchItems.Count)
                throw new NHRedisException();

            var puttableScratchItems = new List<ScratchCacheItem>();
            for (int i = 0; i < currentItemsRaw.Length; ++i)
            {
                var scratch = scratchItems[i];
                var currentObject = client.Deserialize(currentItemsRaw[i]);
                scratch.CurrentCacheValue = currentObject;
                var currentLockableCachedItem = currentObject as LockableCachedItem;

                // this should never happen....
                if (currentObject != null && currentLockableCachedItem == null)
                    throw new NHRedisException();

                var value = scratch.PutParameters.Value;
                var version = scratch.PutParameters.Version;
                var versionComparator = scratch.PutParameters.VersionComparer;

                LockableCachedItem newItem = null;
                if (currentLockableCachedItem == null)
                    newItem = new LockableCachedItem(value, version);
                else if (currentLockableCachedItem.IsPuttable(0, version, versionComparator))
                {
                    currentLockableCachedItem.Update(value, version, versionComparator);
                    newItem = currentLockableCachedItem;
                }
                scratch.NewCacheItemRaw = client.Serialize(newItem);
                if (scratch.NewCacheItemRaw != null)
                    puttableScratchItems.Add(scratch);
                
            }
            return puttableScratchItems;
        }
         

        /// <summary>
        /// Remove item corresponding to key from cache
        /// </summary>
        /// <param name="key"></param>
        public void Remove(object key)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			if (Log.IsDebugEnabled)
				Log.DebugFormat("removing item {0}", key);
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(r => ((RedisNativeClient)r).Del(_cacheNamespace.GlobalCacheKey(key)));
                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                       x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not delete key: {0}", key);
                throw;
            }  
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
		}
 
        /// <summary>
        /// clear cache region
        /// </summary>
		public virtual void Clear()
		{
            //rename set of keys, and Start expiring the keys
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
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
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(
                        r =>
                        ((CustomRedisClient) r).Lock(_cacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey)));
                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                      x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not acquire lock for key: {0}", key);
                throw;
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public void Unlock(object key)
        {
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(
                        r =>
                        ((CustomRedisClient)r).Unlock(_cacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey)));
                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                      x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not release lock for key: {0}", key);
                throw;
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
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
            var rc = new Dictionary<object, object>();
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
              
                long generationFromServer = -1;
                byte[][] resultBytesArray = null;
                var keyCount = 0;
                using (var pipe = client.CreatePipeline())
                {
                    var globalKeys = new List<string>();

                    //generate global keys
                    foreach (var key in keys)
                    {
                        keyCount++;
                        globalKeys.Add(_cacheNamespace.GlobalCacheKey(key));
                    }

                    pipe.QueueCommand(r => ((RedisNativeClient) r).MGet(globalKeys.ToArray()),
                                      x => resultBytesArray = x);

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                      x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                }
                while (generationFromServer != _cacheNamespace.GetGeneration())
                {
                    //update cached generation value, and try again
                    _cacheNamespace.SetGeneration(generationFromServer);

                    using (var pipe = client.CreatePipeline())
                    {
                        var globalKeys = new List<string>();

                        //generate global keys
                        keyCount = 0;
                        foreach (var key in keys)
                        {
                            keyCount++;
                            globalKeys.Add(_cacheNamespace.GlobalCacheKey(key));
                        }

                        pipe.QueueCommand(r => ((RedisNativeClient)r).MGet(globalKeys.ToArray()),
                                          x => resultBytesArray = x);

                        pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                          x => generationFromServer = Convert.ToInt64(x));
                        pipe.Flush();
                    }
                }
                if (keyCount != resultBytesArray.Length)
                    throw new RedisException("MultiGet: number of results does not match number of keys");

                //process results
                var iter = keys.GetEnumerator();
                iter.MoveNext();
                foreach (var resultBytes in resultBytesArray)
                {
                    if (resultBytes != null)
                    {
                        var currentObject = client.Deserialize(resultBytes);
                        if (currentObject != null)
                            rc[iter.Current] = currentObject;
                    }
                    iter.MoveNext();
                }
                return rc;
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IList SMembers(object key)
        {
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    byte[][] members = null;
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(r => ((RedisNativeClient)r).SMembers(_cacheNamespace.GlobalCacheKey(key)),
                                                    x => members = x);

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                                    x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                    var rc = new ArrayList();
                    foreach (var item in members)
                    {
                        rc.Add(disposable.Client.Deserialize(item));
                    }
                    return rc;
                }
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SAdd(object key, object value)
        {
            int rc = 0;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    var bytes = client.Serialize(value);
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(r => disposable.Client.SAdd(_cacheNamespace.GlobalCacheKey(key), bytes), x => rc = x);

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                   x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                }
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
            return rc == 1;
        }
 
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
        public bool SAdd(object key, IList keys)
        {
            IRedisPipeline pipe = null;
            bool success = false;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                
                    var client = disposable.Client;
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    foreach (var k in keys)
                    {
                        var bytes = client.Serialize(k);
                        pipe.QueueCommand(r => ((RedisNativeClient)r).SAdd(_cacheNamespace.GlobalCacheKey(key), bytes), x => success &= x == 1  );
                    }

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                   x => generationFromServer = Convert.ToInt64(x));

                    success = true;
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        success = true;
                        pipe.Replay();
                    }
                }
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
            return success;
       }

        public bool SRemove(object key, object value)
        {
            int rc = 0;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    var bytes = client.Serialize(value);
                    long generationFromServer = -1;
                    pipe = client.CreatePipeline();

                    pipe.QueueCommand(r => ((RedisNativeClient)r).SRem(_cacheNamespace.GlobalCacheKey(key), bytes), 
                                                    x => rc = x);

                    pipe.QueueCommand(r => r.GetValue(_cacheNamespace.GetGenerationKey()),
                                                    x => generationFromServer = Convert.ToInt64(x));
                    pipe.Flush();

                    while (generationFromServer != _cacheNamespace.GetGeneration())
                    {
                        //update cached generation value, and try again
                        _cacheNamespace.SetGeneration(generationFromServer);

                        pipe.Replay();
                    }
                }
            }
            finally
            {
                if (pipe != null)
                    pipe.Dispose();
            }
            return rc == 1;
        }

        #endregion


 
        /// <summary>
        /// hit server for cache _region generation
        /// </summary>
        /// <returns></returns>
        private long FetchGeneration()
        {
            long rc;
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

	}
}