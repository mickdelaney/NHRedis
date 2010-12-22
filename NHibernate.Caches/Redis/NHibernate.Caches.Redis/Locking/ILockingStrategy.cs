using System;

namespace NHibernate.Caches.Redis.Locking
{
    public interface ILockingStrategy
    {
        IDisposable ReadLock();

        IDisposable WriteLock();
    }
}
