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
using System.Text;
using ServiceStack.Redis;
using NHibernate.Cache;
using ServiceStack.Redis.Support.Queue.Implementation;

namespace NHibernate.Caches.Redis
{
	/// <summary>
	/// Cache provider for Redis
	/// </summary>
	public class RedisProvider : ICacheProvider
	{
		private static readonly IInternalLogger Log;
		private static readonly RedisConfig Config;
        private PooledRedisClientManager _clientManager;
		private static readonly object SyncObject = new object();

        private static RedisGarbageCollector _garbageCollector;



        public static string NoClearPropertyKey = "no_clear_on_client";

		static RedisProvider()
		{
			Log = LoggerProvider.LoggerFor(typeof (RedisProvider));
            Config = ConfigurationManager.GetSection("redis") as RedisConfig;
			if (Config == null)
			{
				Log.Info("redis configuration section not found, using default configuration (127.0.0.1:6379).");
				Config = new RedisConfig("localhost",6379);
    		}
           

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
			if (Log.IsDebugEnabled)
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
				Log.Debug("building cache with region: " + regionName + ", properties: " + sb);
			}

		    var noClearClient = true;
            if (properties.ContainsKey(NoClearPropertyKey))
                noClearClient = properties[NoClearPropertyKey] == "true";
          
            if (noClearClient)
                return new NhRedisClientNoClear(regionName, null, properties, _clientManager);
            return new NhRedisClient(regionName, null, properties, _clientManager);
		}


		public long NextTimestamp()
		{
			return Timestamper.Next();
		}

	    public void Start(IDictionary<string, string> properties)
		{
			// Needs to lock staticly because the pool and the internal maintenance thread
			// are both static, and I want them syncs between starts and stops.
			lock (SyncObject)
			{
				if (Config == null)
				{
					throw new ConfigurationErrorsException("Configuration for NHRedis not found");
				}


                if (_clientManager == null)
                {

                    var poolConfig = new RedisClientManagerConfig
                                         {
                                             MaxReadPoolSize = Config.MaxReadPoolSize,
                                             MaxWritePoolSize = Config.MaxWritePoolSize
                                         };

                    _clientManager = new PooledRedisClientManager(new List<string>() { Config.Host },
                                                    new List<string>(), poolConfig)
                                         {
                                             RedisClientFactory = new SerializingRedisClientFactory()
                                         };
                }
                if (_garbageCollector == null)
                {
                   // Note: garbage collections disabled because we are using the optimized NHRedis client that
                   // never clears, so no gc needed
                   // _garbageCollector = new RedisGarbageCollector(Config.Host, Config.Port);
                   // _garbageCollector.Start();
                }
               
			}
		}

		public void Stop()
		{
			lock (SyncObject)
			{
                _clientManager.Dispose();
                _clientManager = null;

                if (_garbageCollector != null)
                {
                    _garbageCollector.Stop();
                    _garbageCollector = null;      
                }
  
			}
		}

		#endregion

     
	}
}