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
using NHibernate.Cache.Query;
using NHibernate.Cache;
using ServiceStack.Redis.Pipeline;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Redis cache client for Redis.
    /// </summary>
	public class NhRedisClient : NhRedisClientBase
	{
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
			: this(regionName, null,  null, properties, null)
		{
		}
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClient(string regionName, IInMemoryQueryProvider inMemoryQueryProvider, string cacheConcurrencyStrategy, IDictionary<string, string> properties, PooledRedisClientManager manager)
            : base(regionName, inMemoryQueryProvider, cacheConcurrencyStrategy, properties,manager)
		{
            //make sure generation is synched with server
            SynchGeneration();

		}


		#region ICache Members
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public override object Get(object key)
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
                    long generationFromServer = _cacheNamespace.GetGeneration();
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
        public override void Put(CachePutParameters putParameters)
		{
            var key = putParameters.Key;
            var value = putParameters.Value;

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
                    long generationFromServer = _cacheNamespace.GetGeneration();
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

 
   
        /// <summary>
        /// Puts a LockedCacheableItem corresponding to (value, version) into
        /// the cache
        /// </summary>
        /// <param name="putParameters"></param>
        public override void Put(IList<CacheVersionedPutParameters> putParameters)
        {
            //deal with null keys
            IList<ScratchCacheItem> scratchItems = new List<ScratchCacheItem>();
            foreach (var putParams in putParameters)
            {
                if (putParams.Key == null) continue;
                scratchItems.Add(new ScratchCacheItem(putParams));
            }
            if (scratchItems.Count == 0)  return;

            byte[][] currentItemsRaw = null;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;

                    long generationFromServer = _cacheNamespace.GetGeneration();

                    pipe = client.CreatePipeline();

                    //watch for changes to generation key and cache key
                    IList<ScratchCacheItem> items = scratchItems;
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(GlobalKeys(items, true)));

                    //get all of the current objects
                    pipe.QueueCommand(r => ((RedisNativeClient)r).MGet(GlobalKeys(items, false)), x => currentItemsRaw = x);

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
                            ScratchCacheItem item = scratch;
                            trans.QueueCommand(
                                r =>
                                ((IRedisNativeClient) r).SetEx(
                                    _cacheNamespace.GlobalCacheKey(item.PutParameters.Key),
                                    _expiry, client.Serialize(item.NewCacheValue)));

                            //add keys to globalKeys set for this namespace
                            trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(),
                                                                   _cacheNamespace.GlobalCacheKey(
                                                                       item.PutParameters.Key)));
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
                                ScratchCacheItem item = scratch;
                                trans.QueueCommand(
                                    r =>
                                    ((IRedisNativeClient)r).SetEx(
                                        _cacheNamespace.GlobalCacheKey(item.PutParameters.Key),
                                        _expiry, client.Serialize(item.NewCacheValue)));

                                //add keys to globalKeys set for this namespace
                                trans.QueueCommand(r => r.AddItemToSet(_cacheNamespace.GetGlobalKeysKey(),
                                                                       _cacheNamespace.GlobalCacheKey(
                                                                           item.PutParameters.Key)));
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
        /// Remove item corresponding to key from cache
        /// </summary>
        /// <param name="key"></param>
        public override void Remove(object key)
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
                    long generationFromServer = _cacheNamespace.GetGeneration();
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
        public override void Clear()
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
        /// <param name="key"></param>
        public override void Lock(object key)
        {
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    long generationFromServer = _cacheNamespace.GetGeneration();
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
        public override void Unlock(object key)
        {
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    long generationFromServer = _cacheNamespace.GetGeneration();
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
        /// Return a dictionary of (key,value) pairs corresponding to a collection of keys
        /// </summary>
        /// <param name="keys"></param>
        /// <returns></returns>
        public override IDictionary MultiGet(IEnumerable keys)
        {
            var rc = new Dictionary<object, object>();
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;

                long generationFromServer = _cacheNamespace.GetGeneration();
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

        #endregion

        private string[] GlobalKeys(IEnumerable<ScratchCacheItem> scratchItems, bool includeGenerationKey)
        {
            var nonNull = new List<string>();
            if (includeGenerationKey)
                nonNull.Add(_cacheNamespace.GetGenerationKey());
            foreach (var item in scratchItems)
            {
                if (item.PutParameters.Key != null)
                    nonNull.Add(_cacheNamespace.GlobalCacheKey(item.PutParameters.Key));
            }
            return nonNull.ToArray();
        }

      
        /// <summary>
        /// 
        /// </summary>
        public override void Destroy()
        {
            Clear();
        }
 
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
	}
}