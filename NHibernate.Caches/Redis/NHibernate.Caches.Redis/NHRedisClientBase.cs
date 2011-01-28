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
using System.Text;
using NHibernate.Cache.Entry;
using NHibernate.Cache.Query;
using ServiceStack.Redis;
using NHibernate.Cache;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Redis.Support;
using ServiceStack.Redis.Support.Queue.Implementation;


namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// base class for Redis cache client. Handles functionality shared between NHRedisClient
    /// and NHRedisClientNoClear, such as namespace management, and live query methods.
    /// </summary>
    public abstract class NhRedisClientBase : AbstractCache, ILiveQueryCache
    {
        protected readonly PooledRedisClientManager ClientManager;

        // manage cache _region        
        protected readonly RedisNamespace CacheNamespace;

        // live query cache region
        private readonly RedisNamespace _liveQueryCacheNamespace = new RedisNamespace(StandardQueryCache.LiveQueryCacheRegionName);
        private readonly RedisNamespace _liveQueriesNamespace = new RedisNamespace(StandardQueryCache.LiveQueriesRegionName);

        protected const string LiveQueriesKey = "LiveQueriesKey";
        protected const string PendingLiveQueriesKey = "PendingLiveQueriesKey";

        protected readonly UTF8Encoding Encoding = new UTF8Encoding();

        protected IDictionary<object, long> AcquiredLocks = new Dictionary<object,long>();

        static NhRedisClientBase()
        {
            Log = LoggerProvider.LoggerFor(typeof(NhRedisClientBase));
        }

        public NhRedisClientBase()
            : this("nhibernate", null)
        {
        }

        public NhRedisClientBase(string regionName)
            : this(regionName, null)
        {
        }

        public NhRedisClientBase(string regionName, IDictionary<string, string> properties)
            : this(regionName, null, null, properties, null)
        {
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClientBase(string regionName, IInMemoryQueryProvider inMemoryQueryProvider, string cacheConcurrencyStrategy, IDictionary<string, string> properties, PooledRedisClientManager manager)
            : base(regionName, inMemoryQueryProvider, cacheConcurrencyStrategy, properties)
        {
            ClientManager = manager;

            var namespacePrefix = _region;
            if (_regionPrefix != null && !_regionPrefix.Equals(""))
                namespacePrefix = _regionPrefix + "_" + _region;
            CacheNamespace = new RedisNamespace(namespacePrefix);

        }

        /// <summary>
        /// New cache item. Null return indicates that we are not allowed to update the cache, due to versioning
        /// </summary>
        /// <param name="currentItemsRaw"></param>
        /// <param name="scratchItems"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public IList<ScratchCacheItem> GenerateNewCacheItems(byte[][] currentItemsRaw, IList<ScratchCacheItem> scratchItems)
        {
            return GenerateNewCacheItems(currentItemsRaw, scratchItems, default(SerializingRedisClient));
        }

        /// <summary>
        /// New cache item. Null return indicates that we are not allowed to update the cache, due to versioning
        /// </summary>
        /// <param name="currentItemsRaw"></param>
        /// <param name="scratchItems"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public IList<ScratchCacheItem> GenerateNewCacheItems(byte[][] currentItemsRaw, IList<ScratchCacheItem> scratchItems, SerializingRedisClient client)
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
                scratch.NewCacheValue = newItem;
                if (scratch.NewCacheValue != null)
                    puttableScratchItems.Add(scratch);

            }
            return puttableScratchItems;
        }

        protected string[] WatchKeys(IEnumerable<ScratchCacheItem> scratchItems, bool includeGenerationKey)
        {
            var keys = Keys(scratchItems, includeGenerationKey);
            return AddWatchKeys(ref keys);
        }

        protected string[] WatchKeys(IEnumerable<ScratchCacheItem> scratchItems)
        {
            var keys = Keys(scratchItems);
            return AddWatchKeys(ref keys);
        }
        private string[] AddWatchKeys(ref string[] keys)
        {
            Array.Resize(ref keys, keys.Length + 2);

            keys[keys.Length - 2] = _liveQueriesNamespace.GlobalCacheKey(LiveQueriesKey);
            keys[keys.Length - 1] = _liveQueriesNamespace.GlobalCacheKey(PendingLiveQueriesKey);

            return keys; 
        }

 
        protected string[] Keys(IEnumerable<ScratchCacheItem> scratchItems)
        {
            return Keys(scratchItems, false);
        }

        protected string[] Keys(IEnumerable<ScratchCacheItem> scratchItems, bool includeGenerationKey)
        {
            var globalKeys = new List<string>();
                            
            if (includeGenerationKey)
                globalKeys.Add(CacheNamespace.GetGenerationKey());
            foreach (var item in scratchItems)
            {
                if (item.PutParameters.Key != null)
                    globalKeys.Add(CacheNamespace.GlobalCacheKey(item.PutParameters.Key));
            }
            return globalKeys.ToArray();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IDisposable GetReadLock()
        {
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override IDisposable GetWriteLock()
        {
            return null;
        }

        #region ILiveQueryCache Members

        public void HSet(object key, object field, IInMemoryQuery value)
        {
            HSetImpl(key,field, value, _liveQueriesNamespace);
        }

        public bool HDelLiveQuery(object key, object field)
        {
            return HDelImpl(key, field, _liveQueriesNamespace);
        }

        public IDictionary<object, IInMemoryQuery> HGetAllLiveQueries(object key)
        {
            return HGetAllImpl<IInMemoryQuery>(key, _liveQueriesNamespace);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IDictionary<object, LiveQueryCacheEntry> HGetAll(object key)
        {
            return HGetAllImpl<LiveQueryCacheEntry>(key, _liveQueryCacheNamespace);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public void HSet(object key, object field, LiveQueryCacheEntry value)
        {
           HSetImpl(key, field, value, _liveQueryCacheNamespace);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keyValues"></param>
        public void HSet(object key, IDictionary<object, LiveQueryCacheEntry> keyValues)
        {
            HSetImpl(key, keyValues, _liveQueryCacheNamespace);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        public bool HDel(object key, object field)
        {
            return HDelImpl(key, field, _liveQueryCacheNamespace);
        }
        #endregion


        #region Implementation

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entityCacheKey"></param>
        /// <param name="client"></param>
        /// <param name="pipe"></param>
        protected void QueueDeleteAll(object entityCacheKey, SerializingRedisClient client, IRedisQueueableOperation pipe)
        {
            //entityCacheKey represents non-global key from entity cache
            // must convert to live query field

            //remove object from all live query hashes
            if (!SupportsLiveQueries()) return;
            foreach (var liveQueryKey in _inMemoryQueryProvider.GetQueries().Keys)
            {
                QueryKey queryKey = liveQueryKey;
                pipe.QueueCommand(r =>
                    ((RedisNativeClient)r).HDel(_liveQueryCacheNamespace.GlobalCacheKey(queryKey), GetFieldBytes(entityCacheKey) ));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="key"></param>
        /// <param name="redisNamespace"></param>
        /// <returns></returns>
        private IDictionary<object, T> HGetAllImpl<T>(object key, RedisNamespace redisNamespace)
        {
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                var members = client.HGetAll(redisNamespace.GlobalCacheKey(key));

                var rc = new Dictionary<object, T>();
                for (var i = 0; i < members.Length; i += 2)
                {
                    var temp = client.Deserialize(members[i + 1]);
                    if (temp is T)
                       rc[GetFieldString(members[i])] = (T)temp;
                    else
                        throw new RedisException(String.Format("HGetAll retrieved object of type {0}; was expecting type {1}", temp.GetType(), typeof(T)));
                }
                return rc;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="value"></param>
        /// <param name="redisNamespace"></param>
        private void HSetImpl(object key, object field, object value, RedisNamespace redisNamespace)
        {
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                client.HSet(redisNamespace.GlobalCacheKey(key), GetFieldBytes(field), client.Serialize(value));
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="keyValues"></param>
        /// <param name="redisNamespace"></param>
        private void HSetImpl<T>(object key, IDictionary<object, T> keyValues, RedisNamespace redisNamespace)
        {
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                var fieldBytes = new byte[keyValues.Count][];
                var valueBytes = new byte[keyValues.Count][];
                var i = 0;
                foreach (var kv in keyValues)
                {
                    fieldBytes[i] = GetFieldBytes(kv.Key);
                    valueBytes[i] = client.Serialize(kv.Value);
                    i++;

                }
                client.HMSet(redisNamespace.GlobalCacheKey(key), fieldBytes, valueBytes);

            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="field"></param>
        /// <param name="redisNamespace"></param>
        /// <returns></returns>
        private bool HDelImpl(object key, object field, RedisNamespace redisNamespace)
        {
            using (var disposable = new PooledRedisClientManager.DisposablePooledClient<SerializingRedisClient>(ClientManager))
            {
                var client = disposable.Client;
                return client.HDel(redisNamespace.GlobalCacheKey(key), GetFieldBytes(field)) == 1;
            }
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="putParameters"></param>
        /// <param name="queryCacheable"></param>
        /// <param name="pipe"></param>
        /// <param name="handleRemove"></param>
        protected void QueueLiveQueryUpdates(CachePutParameters putParameters, object queryCacheable, IRedisQueueableOperation pipe, bool handleRemove)
        {
            /*
            // update live query cache
            if (!SupportsLiveQueries()) return;
            foreach (var query in DirtyQueryKeys(putParameters.HydratedObject))
            {
                if (query.IsDirty)
                {
                    var key = query;
                    pipe.QueueCommand(
                        r => ((IRedisNativeClient)r).SAdd(
                                 _liveQueryCacheNamespace.GlobalCacheKey(key.Key),
                                 ((SerializingRedisClient)r).Serialize(queryCacheable)));
                }
                else if (handleRemove)
                {
                    var key = query;
                    pipe.QueueCommand(
                        r => ((IRedisNativeClient)r).SRem(
                                 _liveQueryCacheNamespace.GlobalCacheKey(key.Key),
                                 ((SerializingRedisClient)r).Serialize(queryCacheable)));
                }
            }*/

        }

        /// <summary>
        /// 
        /// </summary>
        public override void Destroy()
        {
            Clear();
        }

        private byte[] GetFieldBytes(object field)
        {
            return Encoding.GetBytes(field.ToString());
        }
        private string GetFieldString(byte[] fieldBytes)
        {
            return Encoding.GetString(fieldBytes);
        }
    }
}

