using System;

namespace NHibernate.Caches.Redis.Locking
{
    public class NoLockingStrategy : ILockingStrategy
    {
        public IDisposable ReadLock()
        {
            return null;
        }

        public IDisposable WriteLock()
        {
            return null;
        }
    }
}
