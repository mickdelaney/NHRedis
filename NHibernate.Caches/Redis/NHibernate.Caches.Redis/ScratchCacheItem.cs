using System;
using NHibernate.Cache;

namespace NHibernate.Caches.Redis
{
    public class ScratchCacheItem
    {
        public CacheVersionedPutParameters PutParameters
        { 
            get; set;
        }
        public object CurrentCacheValue
        {
            get; set;
        }
        public byte[] NewCacheValueRaw
        {
            get;
            set;
        }
        public object NewCacheValue
        {
            get;
            set;
        }
        public ScratchCacheItem(CacheVersionedPutParameters putParameters)
        {
            PutParameters = putParameters;
        }
        public override string ToString()
        {
            return "ScratchCacheItem: " + PutParameters +
                   ", current cache value = " + CurrentCacheValue +
                   ", new cache value = " + NewCacheValue +
                   "}";
        }
    }
}
