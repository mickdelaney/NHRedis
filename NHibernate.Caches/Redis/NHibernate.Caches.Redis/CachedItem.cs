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

namespace NHibernate.Caches.Redis
{
    /// <summary>
    /// An item of cached data, along with its version (which could be null)
    /// </summary>
    [Serializable]
    public class CachedItem
    {
        protected object value;
        protected object version;

        public CachedItem()
        {

        }

        public CachedItem(object value, object version)
        {
            this.value = value;
            this.version = version;

        }
        /// <summary>
        /// The actual cached data
        /// </summary>
        public object Value
        {
            get { return value; }
        }
        /// <summary>
        /// The version of the item
        /// </summary>
        public object Version
        {
            get { return version; }
        }

        /// <summary>
        /// Don't overwrite already cached items
        /// </summary>
        /// <param name="txTimestamp"></param>
        /// <param name="newVersion"></param>
        /// <param name="comparator"></param>
        /// <returns></returns>
        public virtual bool IsPuttable(long txTimestamp, object newVersion, IComparer comparator)
        {
            return comparator != null && comparator.Compare(version, newVersion) < 0;
        }

        public override string ToString()
        {
            return "CachedItem{ version= " + version +
                   ", value= " + value +
                   "}";
        }
    }
}