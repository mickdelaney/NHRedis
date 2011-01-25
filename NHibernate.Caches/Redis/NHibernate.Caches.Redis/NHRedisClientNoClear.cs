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
using NHibernate.Cache.Query;
using ServiceStack.Redis;
using NHibernate.Cache;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Redis.Support;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Redis cache client for Redis.
    /// </summary>
    public class NhRedisClientNoClear : NhRedisClientBase
    {
        static NhRedisClientNoClear()
        {
            Log = LoggerProvider.LoggerFor(typeof(NhRedisClientNoClear));
        }

        public NhRedisClientNoClear()
            : this("nhibernate", null)
        {
        }

        public NhRedisClientNoClear(string regionName)
            : this(regionName, null)
        {
        }

        public NhRedisClientNoClear(string regionName, IDictionary<string, string> properties)
            : this(regionName, null, null, properties, null)
        {
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClientNoClear(string regionName, IInMemoryQueryProvider inMemoryQueryProvider, string cacheConcurrencyStrategy, IDictionary<string, string> properties, PooledRedisClientManager manager)
            : base(regionName, inMemoryQueryProvider, cacheConcurrencyStrategy, properties,manager)
        {
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

            object rc;
            try
            {
                using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
                {
                    var client = disposable.Client;
                    var maybeObj = client.Get(CacheNamespace.GlobalCacheKey(key));
                    rc = (maybeObj == null) ? null : client.Deserialize(maybeObj);
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
        /// <param name="putParameters"></param>
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
                using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
                {
                    var client = disposable.Client;
                    var globalKey = CacheNamespace.GlobalCacheKey(key);

                    using (var pipe = client.CreatePipeline())
                    {
                        pipe.QueueCommand(r => ((IRedisNativeClient)r).SetEx(globalKey, _expiry, client.Serialize(value)));
                     
                        QueueLiveQueryUpdates(putParameters, key, pipe, false);

                        pipe.Flush();
                    }
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not put {0} for key {1}", value, key);
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
                if (Log.IsDebugEnabled)
                    Log.DebugFormat("fetching object {0} from the cache", putParams.Key.ToString());
            }
            if (scratchItems.Count == 0) return;

            byte[][] currentItemsRaw = null;
            IRedisPipeline pipe = null;
            try
            {
                using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
                {
                    var client = disposable.Client;

                    pipe = client.CreatePipeline();

                    //watch for changes to cache keys
                    IList<ScratchCacheItem> items = scratchItems;
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(WatchKeys(items)));

                    //get all of the current objects
                    pipe.QueueCommand(r => ((RedisNativeClient)r).MGet(Keys(items)), x => currentItemsRaw = x);

                    pipe.Flush();

                    // check if there is are new cache items to put
                    scratchItems = GenerateNewCacheItems(currentItemsRaw, scratchItems, client);
                    if (scratchItems.Count == 0)
                        return;

                    bool success;

                    // try to put new items in cache
                    using (var trans = client.CreateTransaction())
                    {

                        foreach (var scratch in scratchItems)
                        {
                            //setex on all new objects
                            ScratchCacheItem item = scratch;
                            trans.QueueCommand(
                                r => ((IRedisNativeClient) r).SetEx(
                                    CacheNamespace.GlobalCacheKey(item.PutParameters.Key),
                                    _expiry, client.Serialize(item.NewCacheValue) ));

                            // update live query cache
                            QueueLiveQueryUpdates(scratch.PutParameters, scratch.PutParameters.Key, trans, true);
                        }
                        success = trans.Commit();
                    }
                    while (!success)
                    {
                        pipe.Replay();

                        // check if there is a new value to put
                        scratchItems = GenerateNewCacheItems(currentItemsRaw, scratchItems, client);
                        if (scratchItems.Count == 0)
                            return;

                        // try to put new items in cache
                        using (var trans = client.CreateTransaction())
                        {

                            foreach (var scratch in scratchItems)
                            {
                                //setex on all new objects
                                ScratchCacheItem item = scratch;
                                trans.QueueCommand(
                                    r =>
                                    ((IRedisNativeClient)r).SetEx(
                                        CacheNamespace.GlobalCacheKey(item.PutParameters.Key),
                                        _expiry, client.Serialize(item.NewCacheValue) ));
                            }

                            //update live query cache 

                            success = trans.Commit();
                        }
                    }
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
        /// clear cache region
        /// </summary>
        public override void Clear()
        {
            // this class is designed around the assumption that clear is never called
            throw new NHRedisException();
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

            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                using (var pipe = client.CreatePipeline())
                {
                      //watch for changes to cache keys
                    pipe.QueueCommand(r => ((RedisNativeClient)r).Del(CacheNamespace.GlobalCacheKey(key)));

                    QueueDeleteAll(key, client, pipe);
                 
                    pipe.Flush();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public override bool Lock(object key)
        {
            bool rc;
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
            {
                long lockExpire = disposable.Client.Lock(CacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey), _lockAcquisitionTimeout, _lockTimeout);
                rc = (lockExpire != 0);
                if (rc)
                    AcquiredLocks[key] = lockExpire;
            }
            return rc;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public override void Unlock(object key)
        {
            if (!AcquiredLocks.ContainsKey(key))
                return;
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
            {
                disposable.Client.Unlock(CacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey), AcquiredLocks[key]);
                AcquiredLocks.Remove(key);
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
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<CustomRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                var globalKeys = new List<string>();

                //generate global keys
                var keyCount = 0;
                foreach (var key in keys)
                {
                    keyCount++;
                    globalKeys.Add(CacheNamespace.GlobalCacheKey(key));
                }
                // do multi get
                var resultBytesArray = client.MGet(globalKeys.ToArray());

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
            }
            return rc;
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        public override void Destroy()
        {
            Clear();
        }

    }
}

