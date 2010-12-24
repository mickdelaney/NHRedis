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
    public class NhRedisClientNoClear : ICache
    {
        private static readonly IInternalLogger Log;
        protected readonly PooledRedisClientManager _clientManager;
        protected readonly int _expiry;

        // NHibernate settings for cache _region and prefix
        protected readonly string _region;
        protected readonly string _regionPrefix;

        // manage cache _region        
        protected readonly RedisNamespace _cacheNamespace;

        static NhRedisClientNoClear()
        {
            Log = LoggerProvider.LoggerFor(typeof(NhRedisClient));
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
            : this(regionName, properties, null)
        {
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClientNoClear(string regionName, IDictionary<string, string> properties, PooledRedisClientManager manager)
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
                return null;
            if (Log.IsDebugEnabled)
                Log.DebugFormat("fetching object {0} from the cache", key);

            object rc;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;
                    var maybeObj = client.Get(_cacheNamespace.GlobalCacheKey(key));
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
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Put(object key, object value)
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
                    var globalKey = _cacheNamespace.GlobalCacheKey(key);

                    ((IRedisNativeClient)client).SetEx(globalKey, _expiry, client.Serialize(value));
                }
            }
            catch (Exception)
            {
                Log.WarnFormat("could not put {0} for key {1}", value, key);
                throw;
            }
        }

        protected string[] GlobalKeys(IEnumerable<ScratchCacheItem> scratchItems, bool includeGenerationKey)
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
        /// Puts a LockedCacheableItem corresponding to (value, version) into
        /// the cache
        /// </summary>
        /// <param name="putParameters"></param>
        public void Put(IList<VersionedPutParameters> putParameters)
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
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;

                    pipe = client.CreatePipeline();

                    //watch for changes to cache keys
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(GlobalKeys(scratchItems, false)));

                    //get all of the current objects
                    pipe.QueueCommand(r => ((RedisNativeClient)r).MGet(GlobalKeys(scratchItems, false)), x => currentItemsRaw = x);

                    pipe.Flush();

                    // check if there is are new cache items to put
                    scratchItems = GenerateNewCacheItems(currentItemsRaw, scratchItems, client);
                    if (scratchItems.Count == 0)
                        return;

                    bool success = false;

                    // try to put new items in cache
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
                                trans.QueueCommand(
                                    r =>
                                    ((IRedisNativeClient)r).SetEx(
                                        _cacheNamespace.GlobalCacheKey(scratch.PutParameters.Key),
                                        _expiry, scratch.NewCacheItemRaw));
                            }

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
        public void Clear()
        {
            // this class is designed around the assumption that clear is never called
            throw new NHRedisException();
        }
        #endregion
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

            using (var disposable = new DisposableClient(_clientManager))
            {
                disposable.Client.Del(_cacheNamespace.GlobalCacheKey(key));
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
            using (var disposable = new DisposableClient(_clientManager))
            {
                disposable.Client.Lock(_cacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        public void Unlock(object key)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                disposable.Client.Unlock(_cacheNamespace.GlobalKey(key, RedisNamespace.NumTagsForLockKey));
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
            get { return Timestamper.OneMs * 60000; }
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
                var globalKeys = new List<string>();

                //generate global keys
                var keyCount = 0;
                // Note: should get generation
                foreach (var key in keys)
                {
                    keyCount++;
                    globalKeys.Add(_cacheNamespace.GlobalCacheKey(key));
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
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IList SMembers(object key)
        {

            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var members = client.SMembers(_cacheNamespace.GlobalCacheKey(key));

                var rc = new ArrayList();
                foreach (var item in members)
                {
                    rc.Add(disposable.Client.Deserialize(item));
                }
                return rc;
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
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var bytes = client.Serialize(value);

                return disposable.Client.SAdd(_cacheNamespace.GlobalCacheKey(key), bytes) == 1;

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keys"></param>
        /// <returns></returns>
        public bool SAdd(object key, IList keys)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                bool success = true;
                var client = disposable.Client;
                foreach (var k in keys)
                {
                    var bytes = client.Serialize(k);
                    success &= client.SAdd(_cacheNamespace.GlobalCacheKey(key), bytes) == 1;
                }
                return success;

            }
        }

        public bool SRemove(object key, object value)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var bytes = client.Serialize(value);

                return  client.SRem(_cacheNamespace.GlobalCacheKey(key), bytes) == 1;

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

