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
    public class NhRedisClientNoClear : NhRedisClient
    {
        private static readonly IInternalLogger Log;

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
            : this(regionName, properties, null)
        {
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="regionName"></param>
        /// <param name="properties"></param>
        /// <param name="manager"></param>
        public NhRedisClientNoClear(string regionName, IDictionary<string, string> properties, PooledRedisClientManager manager) : base(regionName,properties,manager)
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
        public override void Put(object key, object value)
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

        private string[] WatchKeys(object key)
        {
            return new[] { _cacheNamespace.GlobalCacheKey(key) };

        }

        /// <summary>
        /// Puts a LockedCacheableItem corresponding to (value, version) into
        /// the cache
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="version"></param>
        /// <param name="versionComparator"></param>
        public override void Put(List<VersionedPutParameters> putParameters)
        {
            var key = putParameters.Key;
            var value = putParameters.Value;
            var version = putParameters.Version;
            var versionComparator = putParameters.VersionComparer;
            if (key == null)
                return;
            if (Log.IsDebugEnabled)
                Log.DebugFormat("fetching object {0} from the cache", key);

            byte[] maybeObj = null;
            IRedisPipeline pipe = null;
            IRedisTransaction trans = null;
            try
            {
                using (var disposable = new DisposableClient(_clientManager))
                {
                    var client = disposable.Client;

                    pipe = client.CreatePipeline();

                    //watch for changes to generation key and cache key
                    pipe.QueueCommand(r => ((RedisClient)r).Watch(WatchKeys(key)));

                    pipe.QueueCommand(r => ((RedisNativeClient)r).Get(_cacheNamespace.GlobalCacheKey(key)),
                                       x => maybeObj = x);

                    pipe.Flush();


                    // check if can we can put this new (value, version) into the cache
                    var bytesToCache = client.Serialize(GenerateNewCachedItem(maybeObj, value, version, versionComparator, client));
                    if (bytesToCache == null)
                        return;

                    // put new item in cache
                    trans = client.CreateTransaction();

                    trans.QueueCommand(r => ((IRedisNativeClient)r).SetEx(_cacheNamespace.GlobalCacheKey(key),
                                                 _expiry, bytesToCache));

                    var success = trans.Commit(); ;
                    while (!success)
                    {
                        pipe.Replay();

                        // check if can we can put this new (value, version) into the cache
                        bytesToCache = client.Serialize(GenerateNewCachedItem(maybeObj, value, version, versionComparator, client));
                        if (bytesToCache == null)
                            return;

                        success = trans.Replay();
                    }
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
            var currentObject = client.Deserialize(maybeObj);
            var currentLockableCachedItem = currentObject as LockableCachedItem;

            // this should never happen....
            if (currentObject != null && currentLockableCachedItem == null)
                throw new NHRedisException();

            if (currentLockableCachedItem == null)
                newItem = new LockableCachedItem(value, version);
            else if (currentLockableCachedItem.IsPuttable(0, version, versionComparator))
            {
                currentLockableCachedItem.Update(value, version, versionComparator);
                newItem = currentLockableCachedItem;
            }
            return newItem;
        }

        /// <summary>
        /// clear cache region
        /// </summary>
        public override void Clear()
        {
            // this class is designed around the assumption that clear is never called
            throw new NHRedisException();
        }
        #endregion

    }
}