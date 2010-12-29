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
        public byte[] NewCacheItemRaw
        {
            get;
            set;
        }
        public ScratchCacheItem(CacheVersionedPutParameters putParameters)
        {
            PutParameters = putParameters;
        }
    }
}
