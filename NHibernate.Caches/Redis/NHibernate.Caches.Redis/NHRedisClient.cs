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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using ServiceStack.Redis;
using NHibernate.Cache;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization;

using Environment = NHibernate.Cfg.Environment;

namespace NHibernate.Caches.Redis
{
	public class NHRedisClient : ICache
	{
		private static readonly IInternalLogger log;
		[ThreadStatic] private static HashAlgorithm hasher;

		[ThreadStatic] private static MD5 md5;
        private readonly IRedisNativeClient client;
		private readonly int expiry;

		private readonly string region;
		private readonly string regionPrefix = "";
        System.IO.MemoryStream _memoryStream = new System.IO.MemoryStream(1024);
        BinaryFormatter bf = new BinaryFormatter();


		static NHRedisClient()
		{
			log = LoggerProvider.LoggerFor(typeof (RedisClient));
		}

		public NHRedisClient()
			: this("nhibernate", null)
		{
		}

		public NHRedisClient(string regionName)
			: this(regionName, null)
		{
		}

		public NHRedisClient(string regionName, IDictionary<string, string> properties)
			: this(regionName, properties, new RedisNativeClient())
		{
		}

        public NHRedisClient(string regionName, IDictionary<string, string> properties, IRedisNativeClient redisClient)
		{
			region = regionName;

			client = redisClient;

			expiry = 300;

			if (properties != null)
			{
				string expirationString = GetExpirationString(properties);
				if (expirationString != null)
				{
					expiry = Convert.ToInt32(expirationString);
					if (log.IsDebugEnabled)
					{
						log.DebugFormat("using expiration of {0} seconds", expiry);
					}
				}

				if (properties.ContainsKey("regionPrefix"))
				{
					regionPrefix = properties["regionPrefix"];
					if (log.IsDebugEnabled)
					{
						log.DebugFormat("new regionPrefix :{0}", regionPrefix);
					}
				}
				else
				{
					if (log.IsDebugEnabled)
					{
						log.Debug("no regionPrefix value given, using defaults");
					}
				}
			}
		}

		private static HashAlgorithm Hasher
		{
			get
			{
				if (hasher == null)
				{
					hasher = HashAlgorithm.Create();
				}
				return hasher;
			}
		}

		private static MD5 Md5
		{
			get
			{
				if (md5 == null)
				{
					md5 = MD5.Create();
				}
				return md5;
			}
		}

		#region ICache Members

		public object Get(object key)
		{
			if (key == null)
			{
				return null;
			}
			if (log.IsDebugEnabled)
			{
				log.DebugFormat("fetching object {0} from the cache", key);
			}
			byte[] maybeObj = client.Get(KeyAsString(key));
			if (maybeObj == null)
			{
				return null;
			}

            _memoryStream.Seek(0, 0);
            _memoryStream.Write(maybeObj, 0, maybeObj.Length);

            _memoryStream.Seek(0, 0);
             DictionaryEntry de = (DictionaryEntry)bf.Deserialize(_memoryStream);
			return de.Value;
		}

		public void Put(object key, object value)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key", "null key not allowed");
			}
			if (value == null)
			{
				throw new ArgumentNullException("value", "null value not allowed");
			}

			if (log.IsDebugEnabled)
			{
				log.DebugFormat("setting value for item {0}", key);
			}
            var dictEntry = new DictionaryEntry(null, value);

            _memoryStream.Seek(0, 0);
            bf.Serialize(_memoryStream, dictEntry);
            byte[] bytes = _memoryStream.GetBuffer();

            client.SetEx(KeyAsString(key), expiry, bytes);

            //todo: check for failure
            /*
            if (log.IsWarnEnabled)
            {
                log.WarnFormat("could not save: {0} => {1}", key, value);
            }
            */

    	}

		public void Remove(object key)
		{
			if (key == null)
			{
				throw new ArgumentNullException("key");
			}
			if (log.IsDebugEnabled)
			{
				log.DebugFormat("removing item {0}", key);
			}
			client.Del(KeyAsString(key));
		}

		public void Clear()
		{
			client.FlushAll();
		}

		public void Destroy()
		{
			Clear();
		}

		public void Lock(object key)
		{
			// do nothing
		}

		public void Unlock(object key)
		{
			// do nothing
		}

		public long NextTimestamp()
		{
			return Timestamper.Next();
		}

		public int Timeout
		{
			get { return Timestamper.OneMs*60000; }
		}

		public string RegionName
		{
			get { return region; }
		}

		#endregion

		private static string GetExpirationString(IDictionary<string, string> props)
		{
			string result;
			if (!props.TryGetValue("expiration", out result))
			{
				props.TryGetValue(Environment.CacheDefaultExpiration, out result);
			}
			return result;
		}

		/// <summary>
		/// Turn the key obj into a string, preperably using human readable
		/// string, and if the string is too long (>=250) it will be hashed
		/// </summary>
		private string KeyAsString(object key)
		{
            return key.ToString();
		}
	}
}