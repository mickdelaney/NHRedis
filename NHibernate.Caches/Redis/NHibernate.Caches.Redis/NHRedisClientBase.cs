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
    public abstract class NhRedisClientBase : AbstractCache
    {
        protected readonly PooledRedisClientManager ClientManager;

        // manage cache _region        
        protected readonly RedisNamespace CacheNamespace;

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
            : this(regionName, null, properties, null)
        {
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="cacheConcurrencyStrategy"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClientBase(string regionName, string cacheConcurrencyStrategy, IDictionary<string, string> properties, PooledRedisClientManager manager)
            : base(regionName, cacheConcurrencyStrategy, properties)
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
            return keys;
        }

        protected string[] WatchKeys(IEnumerable<ScratchCacheItem> scratchItems)
        {
            var keys = Keys(scratchItems);
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

    }
}

