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
using System.Text;
using NHibernate.Cache.Query;
using ServiceStack.Redis;
using NHibernate.Cache;
using ServiceStack.Redis.Pipeline;
using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// Redis cache client for Redis.
    /// </summary>
    public abstract class NhRedisClientBase : AbstractCache, ILiveQueryCache
    {
        protected readonly PooledRedisClientManager _clientManager;

        // manage cache _region        
        protected readonly RedisNamespace _cacheNamespace;

        // live query cache region
        protected readonly RedisNamespace _liveQueryCacheNamespace = new RedisNamespace(StandardQueryCache.LiveQueryCacheRegionName);

        protected readonly UTF8Encoding encoding = new UTF8Encoding();

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
            _clientManager = manager;

            var namespacePrefix = _region;
            if (_regionPrefix != null && !_regionPrefix.Equals(""))
                namespacePrefix = _regionPrefix + "_" + _region;
            _cacheNamespace = new RedisNamespace(namespacePrefix);

        }
 
        /// <summary>
        /// New cache item. Null return indicates that we are not allowed to update the cache, due to versioning
        /// </summary>
        /// <param name="currentItemsRaw"></param>
        /// <param name="scratchItems"></param>
        /// <param name="client"></param>
        /// <returns></returns>
        public IList<ScratchCacheItem> GenerateNewCacheItems(byte[][] currentItemsRaw, IList<ScratchCacheItem> scratchItems, CustomRedisClient client)
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
         
        protected string[] GlobalKeys(IEnumerable<ScratchCacheItem> scratchItems)
        {
            var nonNull = new List<string>();
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IList HGetAll(object key)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var members = client.HGetAll(_liveQueryCacheNamespace.GlobalCacheKey(key));

                var rc = new ArrayList();
                for (int i = 0; i < members.Length; i+=2 )
                {
                    rc.Add(encoding.GetString(members[i]));
                    rc.Add(disposable.Client.Deserialize(members[i+1]));
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
        public bool HSet(object key, object field, object value)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                return client.HSet(_liveQueryCacheNamespace.GlobalCacheKey(key), encoding.GetBytes(field.ToString()), client.Serialize(value)) == 1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool HSet(object key, IList fields, IList values)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                var fieldBytes = new byte[fields.Count][];
               
                for (int i = 0; i < fields.Count; ++i)
                {
                    fieldBytes[i] = encoding.GetBytes(fields[i].ToString());

                }
                var valueBytes = new byte[values.Count][];
                for (int i = 0; i < values.Count; ++i)
                {
                    valueBytes[i] = client.Serialize(values[i]);

                }
                client.HMSet(_liveQueryCacheNamespace.GlobalCacheKey(key), fieldBytes, valueBytes);
                return true;

            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool HDel(object key, object field)
        {
            using (var disposable = new DisposableClient(_clientManager))
            {
                var client = disposable.Client;
                return client.HDel(_liveQueryCacheNamespace.GlobalCacheKey(key), encoding.GetBytes(field.ToString())) == 1;
            }
        }
        #endregion

        protected void QueueLiveQueryUpdates(object hydratedObject, object cacheKey, IRedisQueueableOperation pipe, bool handleRemove)
        {

            // update live query cache
            if (!SupportsLiveQueries()) return;
            foreach (var query in DirtyQueryKeys(hydratedObject))
            {
                if (query.IsDirty)
                {
                    DirtyQueryKey key = query;
                    pipe.QueueCommand(
                        r => ((IRedisNativeClient)r).SAdd(
                                 _liveQueryCacheNamespace.GlobalCacheKey(key.Key),
                                 ((CustomRedisClient)r).Serialize(cacheKey)));
                }
                else if (handleRemove)
                {
                    DirtyQueryKey key = query;
                    pipe.QueueCommand(
                        r => ((IRedisNativeClient)r).SRem(
                                 _liveQueryCacheNamespace.GlobalCacheKey(key.Key),
                                 ((CustomRedisClient)r).Serialize(cacheKey)));
                }
            }

        }


        /// <summary>
        /// 
        /// </summary>
        public override void Destroy()
        {
            Clear();
        }
    }
}

