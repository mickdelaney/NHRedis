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

using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Text;
using ServiceStack.Redis;
using NHibernate.Cache;
using NHibernate.Caches.Redis;

namespace NHibernate.Caches.Redis
{
	/// <summary>
	/// Cache provider for Redis
	/// </summary>
	public class RedisProvider : ICacheProvider
	{
		private static readonly IInternalLogger log;
		private static readonly RedisConfig config;
        private PooledRedisClientManager clientManager;
		private static readonly object syncObject = new object();

        private static RedisGarbageCollector garbageCollector;


		static RedisProvider()
		{
			log = LoggerProvider.LoggerFor(typeof (RedisProvider));
            config = ConfigurationManager.GetSection("redis") as RedisConfig;
			if (config == null)
			{
				log.Info("redis configuration section not found, using default configuration (127.0.0.1:6379).");
				config = new RedisConfig("localhost",6379);
    		}
            garbageCollector = new RedisGarbageCollector(config.Host, config.Port);

		}

		#region ICacheProvider Members

		public ICache BuildCache(string regionName, IDictionary<string, string> properties)
		{
			if (regionName == null)
			{
				regionName = "";
			}
			if (properties == null)
			{
				properties = new Dictionary<string, string>();
			}
			if (log.IsDebugEnabled)
			{
				var sb = new StringBuilder();
				foreach (var pair in properties)
				{
					sb.Append("name=");
					sb.Append(pair.Key);
					sb.Append("&value=");
					sb.Append(pair.Value);
					sb.Append(";");
				}
				log.Debug("building cache with region: " + regionName + ", properties: " + sb);
			}


            return new NhRedisClient(regionName, properties, clientManager);
		}

		public long NextTimestamp()
		{
			return Timestamper.Next();
		}

		public void Start(IDictionary<string, string> properties)
		{
			// Needs to lock staticly because the pool and the internal maintenance thread
			// are both static, and I want them syncs between starts and stops.
			lock (syncObject)
			{
				if (config == null)
				{
					throw new ConfigurationErrorsException("Configuration for enyim.com/memcached not found");
				}


                if (clientManager == null)
                {

                    RedisClientManagerConfig poolConfig = new RedisClientManagerConfig();
                    poolConfig.MaxReadPoolSize = config.MaxReadPoolSize;
                    poolConfig.MaxWritePoolSize = config.MaxWritePoolSize;

                    List<string> readWrite = new List<string>() { config.Host };
                    clientManager = new PooledRedisClientManager(new List<string>() { config.Host },
                                                    new List<string>(), poolConfig);

                }
                garbageCollector.Start();
			}
		}

		public void Stop()
		{
			lock (syncObject)
			{
                clientManager.Dispose();
                clientManager = null;

                garbageCollector.Stop();
			}
		}

		#endregion
	}
}