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


using System;
using System.Collections;
﻿using NHibernate.Cache;

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// An item of cached data used for copy on write. 
    /// Note: no need for hard locks when accessing any methods, because only one session should be managing this item
    /// </summary>
    [Serializable]
    public class LockableCachedItem : CachedItem
    {

        private long _lockCount = 1;

        public LockableCachedItem()
        {

        }
        public LockableCachedItem(object value, object version)
            : base(value, version)
        {


        }


        /// <summary>
        /// Lock the item
        /// </summary>
        public long Lock()
        {
            _lockCount++;
            return _lockCount;
        }

        /// <summary>
        /// Unlock the item
        /// </summary>
        public long Unlock()
        {
            _lockCount--;
            return _lockCount;
        }
        /// <summary>
        /// Get the lock count
        /// </summary>
        public long LockCount()
        {
            return _lockCount;
        }


        /// <summary>
        /// A LockableCachedItem can be created with default constructor, as a placeholder in the local cache.
        /// But, we don't want to treat this as an updated value.
        /// </summary>
        public bool Updated
        {
            get { return version != null; }
        }


        /// <summary>
        /// </summary>
        /// <param name="newValue"></param>
        /// <param name="newVersion"></param>
        /// <param name="newComparator"></param>
        /// <returns></returns>
        public bool Update(object newValue, object newVersion, IComparer newComparator)
        {
            if (newVersion == null || newComparator == null)
                throw new CacheException("Trying to update copy on write cached item with null version or null _comparator.");

            // note: canUpdate will be true if version is null, which is what we want
            bool canUpdate = newComparator.Compare(version, newVersion) < 0;
            if (canUpdate)
            {
                value = newValue;
                version = newVersion;
            }
            return canUpdate;
        }

        public override string ToString()
        {
            return "LockableCachedItem{version= " + version +
                   ", value= " + value +
                   ", lock count= " + _lockCount +
                   "}";
        }

    }
}