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

﻿using System;
using System.Collections;
using System.Collections.Generic;
﻿using NHibernate.Cache;
using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.Caches.Redis
{
    public abstract class AbstractCache : ICache
    {
        protected static IInternalLogger Log;

        protected readonly int _expiry;
        protected readonly int _lockAcquisitionTimeout;
        protected readonly int _lockTimeout;

        // NHibernate settings for cache _region and prefix
        protected readonly string _region;
        protected readonly string _regionPrefix;

        protected string _concurrencyStrategy;

        public static string ExpirationPropertyKey = "expiration";
        public static string LockAcquisitionTimeoutPropertyKey = "lock_acquisition_timeout";
        public static string LockTimeoutPropertyKey = "lock_timeout";

        public AbstractCache()
            : this("nhibernate",  null, null)
        {
        }

        public AbstractCache(string regionName)
            : this(regionName, null, null)
        {
        }


        public AbstractCache(string regionName,  string cacheConcurrencyStragey, IDictionary<string, string> properties)
        {
            _concurrencyStrategy = cacheConcurrencyStragey;
            _region = regionName;
            _expiry = 300;
            _lockAcquisitionTimeout = 30;
            _lockTimeout = 30;

            if (properties == null) return;

            string propString;
            if (!properties.TryGetValue("expiration", out propString))
            {
                properties.TryGetValue(Environment.CacheDefaultExpiration, out propString);
            }

            if (propString != null)
            {
                _expiry = Convert.ToInt32(propString);
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("using expiration of {0} seconds", _expiry);
                }
            }




            if (properties.ContainsKey("_regionPrefix"))
            {
                _regionPrefix = properties["_regionPrefix"];
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



        #region ICache Members

        public abstract object Get(object key);
        public abstract void Put(object key, object value);
        public abstract void Remove(object key);
        public abstract void Clear();
        public abstract void Destroy();
        public abstract void Lock(object key);
        public abstract void Unlock(object key);
        public abstract IDisposable GetReadLock();
        public abstract IDisposable GetWriteLock();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual long NextTimestamp()
        {
            return Timestamper.Next();
        }
        /// <summary>
        /// 
        /// </summary>
        public virtual int Timeout
        {
            get { return Timestamper.OneMs * 60000; }
        }
        /// <summary>
        /// 
        /// </summary>
        public virtual string RegionName
        {
            get { return _region; }
        }

        public abstract IDictionary MultiGet(IEnumerable keys);

        #endregion




    }
}