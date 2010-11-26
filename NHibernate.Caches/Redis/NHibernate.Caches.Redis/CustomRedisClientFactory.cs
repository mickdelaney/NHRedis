using ServiceStack.Redis;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClientFactory : IRedisClientFactory
    {
        public static CustomRedisClientFactory Instance = new CustomRedisClientFactory();

        public RedisClient CreateRedisClient(string host, int port)
        {
            return new CustomRedisClient(host, port);
        }
    }
}
