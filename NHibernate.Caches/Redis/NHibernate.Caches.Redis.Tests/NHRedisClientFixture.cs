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
using System.Threading;
using Iesi.Collections;
using log4net.Config;
using NHibernate.Cache;
using NUnit.Framework;

namespace NHibernate.Caches.Redis.Tests
{
	public class NhRedisClientFixture
	{
		protected Dictionary<string, string> _props;
		protected ICacheProvider _provider;

		[TestFixtureSetUp]
		public virtual void FixtureSetup()
		{
			XmlConfigurator.Configure();
			_props = new Dictionary<string, string> {{RedisProvider.NoClearPropertyKey, "false"}, 
                                                     {AbstractCache.ExpirationPropertyKey, "20"},
                                                    {AbstractCache.LockAcquisitionTimeoutPropertyKey, "1"},
                                                     {AbstractCache.LockTimeoutPropertyKey, "20"}
            };
          
			_provider = new RedisProvider();
			_provider.Start(_props);
		}

		[TestFixtureTearDown]
		public void FixtureStop()
		{
			_provider.Stop();
		}

		[Test]
		public virtual void TestClear()
		{
			var key = "key1";
			var value = "value";

			var cache = _provider.BuildCache("nunit", _props);
			Assert.IsNotNull(cache, "no cache returned");

			// add the item
			cache.Put(key, value );
			Thread.Sleep(1000);

			// make sure it's there
			var item = cache.Get(key);
			Assert.IsNotNull(item, "couldn't find item in cache");

			// clear the cache
			cache.Clear();

			// make sure we don't get an item
			item = cache.Get(key);
			Assert.IsNull(item, "item still exists in cache");
		}

		[Test]
		public void TestDefaultConstructor()
		{
			ICache cache = new NhRedisClient();
			Assert.IsNotNull(cache);
		}

		[Test]
		public void TestEmptyProperties()
		{
            ICache cache = new NhRedisClient("nunit", new Dictionary<string, string>());
			Assert.IsNotNull(cache);
		}

		[Test]
		public void TestNoPropertiesConstructor()
		{
            ICache cache = new NhRedisClient("nunit");
			Assert.IsNotNull(cache);
		}

		[Test]
		public void TestNullKeyGet()
		{
            var cache = _provider.BuildCache("nunit", _props);
			cache.Put("nunit", "value");
			Thread.Sleep(1000);
			var item = cache.Get(null);
			Assert.IsNull(item);
		}

		[Test]
		public void TestNullKeyPut()
		{
            ICache cache = new NhRedisClient();
			Assert.Throws<ArgumentNullException>(() => cache.Put(null, null));
		}

		[Test]
		public void TestNullKeyRemove()
		{
            ICache cache = new NhRedisClient();
			Assert.Throws<ArgumentNullException>(() => cache.Remove(null));
		}

		[Test]
		public void TestNullValuePut()
		{
            ICache cache = new NhRedisClient();
			Assert.Throws<ArgumentNullException>(() => cache.Put("nunit", null ));
		}

        [Serializable]
        public class SimpleComparer : IComparer
        {

            public int Compare(Object x, Object y)
            {
                if (x == null && y == null)
                    return 0;
                if (x == null)
                    return int.MinValue;
                else if (y == null)
                    return int.MaxValue;
                else
                    return (int)x - (int)y;
            }

        }
     
 [Test]
		public void TestRegions()
		{
			const string key = "key";
            var cache1 = _provider.BuildCache("nunit1", _props);
            var cache2 = _provider.BuildCache("nunit2", _props);
			const string s1 = "test1";
			const string s2 = "test2";
			cache1.Put( key, s1 );
			cache2.Put( key, s2 );
			Thread.Sleep(1000);
			var get1 = cache1.Get(key);
			var get2 = cache2.Get(key);
			Assert.IsFalse(get1 == get2);
		}

		[Test]
		public void TestRemove()
		{
			const string key = "key1";
			const string value = "value";

            var cache = _provider.BuildCache("nunit", _props);
			Assert.IsNotNull(cache, "no cache returned");

			// add the item
			cache.Put(key, value);
			Thread.Sleep(1000);

			// make sure it's there
			var item = cache.Get(key);
			Assert.IsNotNull(item, "item just added is not there");

			// remove it
			cache.Remove(key);

			// make sure it's not there
			item = cache.Get(key);
			Assert.IsNull(item, "item still exists in cache");
		}
	}
}