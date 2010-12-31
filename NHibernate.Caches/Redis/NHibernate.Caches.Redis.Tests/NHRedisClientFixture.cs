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
using Iesi.Collections.Generic;
using log4net.Config;
using NHibernate.Cache;
using NHibernate.Engine;
using NHibernate.Impl;
using NHibernate.SqlCommand;
using NHibernate.Util;
using NUnit.Framework;

namespace NHibernate.Caches.Redis.Tests
{
	public class NhRedisClientFixture
	{
		protected Dictionary<string, string> _props;
		protected ICacheProvider _provider;

        public class TestInMemoryQuery : IInMemoryQuery
        {
            public bool Match(object entity)
            {
                return true;
            }
        }

        public class TestInMemoryQueryProvider : IInMemoryQueryProvider
	    {
            private readonly IThreadSafeDictionary<QueryKey, IInMemoryQuery> _queries = new ThreadSafeDictionary<QueryKey, IInMemoryQuery>();

            public TestInMemoryQueryProvider()
            {
                var qk = new QueryKey(null, new SqlString("1=1"), new QueryParameters(), new HashedSet());
                _queries[qk] = new TestInMemoryQuery();
            }

            public IThreadSafeDictionary<QueryKey, IInMemoryQuery> GetQueries()
            {
                return _queries;
            }
	    }

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			XmlConfigurator.Configure();
			_props = new Dictionary<string, string> {{RedisProvider.NoClearPropertyKey, "false"}, 
                                                     {RedisProvider.ExpirationPropertyKey, "20"}};
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

			var cache = _provider.BuildCache("nunit", new TestInMemoryQueryProvider(), null, _props);
			Assert.IsNotNull(cache, "no cache returned");

			// add the item
			cache.Put(new CachePutParameters(null, key, value) );
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
            var cache = _provider.BuildCache("nunit", new TestInMemoryQueryProvider(), null, _props);
			cache.Put(new CachePutParameters(null, "nunit", "value") );
			Thread.Sleep(1000);
			var item = cache.Get(null);
			Assert.IsNull(item);
		}

		[Test]
		public void TestNullKeyPut()
		{
            ICache cache = new NhRedisClient();
			Assert.Throws<ArgumentNullException>(() => cache.Put(new CachePutParameters()));
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
			Assert.Throws<ArgumentNullException>(() => cache.Put(new CachePutParameters(null, "nunit", null) ));
		}

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
        public void TestSAdd()
        {
            var key = "key";
            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);
            Assert.IsFalse(cache.SRemove("key", "value"));
            Assert.IsTrue(cache.SAdd("key", "value"));
            Assert.IsTrue(cache.SRemove("key","value"));
        }

        [Test]
        public void TestSAddMultiple()
        {
            string key = "key";
            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);
            cache.Remove(key);
            var vals = new string[]{"value1", "value2"};
            Assert.IsFalse(cache.SRemove(key, vals[0]));
            Assert.IsFalse(cache.SRemove(key, vals[1]));
            bool rc = cache.SAdd(key, vals);
            Assert.IsTrue(cache.SRemove(key, vals[0]));
            Assert.IsTrue(cache.SRemove(key, vals[1]));
        }

        
        [Test]
        public void TestSMembers()
        {
            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);

            string key = "key";
            cache.Remove(key);

            var vals = new ArrayList()
                                    {
                                        "value1", "value2", "value3"
                                    };
            for (int i = 0; i < vals.Count; ++i)
                cache.SAdd(key, vals[i]);

            IEnumerable members = cache.SMembers(key);
            foreach (var member in members)
            {
                Assert.IsTrue(vals.Contains(member));
            }
            
            
         }

	    [Test]
        public void TestMultiGet()
        {
            var cache = _provider.BuildCache(typeof(String).FullName, new TestInMemoryQueryProvider(), null, _props);

            List<string> keys = new List<string>()
                                    {
                                        "key1", "key2", "key3"
                                    };


            List<string> vals = new List<string>()
                                    {
                                        "value1", "value2", "value3"
                                    };
            for (int i = 0; i < keys.Count; ++i )
                cache.Put(new CachePutParameters(null, keys[i], vals[i]));

            IDictionary pre = cache.MultiGet(keys);
            for (int i = 0; i < keys.Count; ++i)
            {
                Assert.IsTrue(pre.Contains(keys[i]));
                Assert.AreEqual(vals[i], pre[keys[i]]);
            }

            //test with "expired" key - at index 1
            cache.Remove(keys[1]);
            pre = cache.MultiGet(keys);
            keys.RemoveAt(1);
            vals.RemoveAt(1);
            for (int i = 0; i < keys.Count; ++i)
            {
                Assert.IsTrue(pre.Contains(keys[i]));
                Assert.AreEqual(vals[i], pre[keys[i]]);
            }

        }

	    [Test]
        public void TestVersionedPut()
        {
            const string key = "key1";

            SimpleComparer comparer = new SimpleComparer();
            var cache = _provider.BuildCache(typeof(String).FullName, new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);

            int version1 = 1;
            string value1 = "value1";

            int version2 = 2;
            string value2 = "value2";

            int version3 = 3;
            string value3 = "value3";

            cache.Remove(key);


            //check if object is cached correctly
            var versionParams =
	        new CacheVersionedPutParameters()
	            {
	                Key = key,
	                Value = value1,
	                Version = version1,
	                VersionComparer = comparer
	            };
	        var list = new List<CacheVersionedPutParameters> {versionParams};
            cache.Put(list);
            var obj = cache.Get(key) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value1);

	        versionParams.Value = value2;
	        versionParams.Version = version2;
            // check that object changes with next version
            cache.Put(list);
            obj = cache.Get(key) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value2);

            // check that older version does not change cache
            versionParams.Value = value3;
            versionParams.Version = version1;
            cache.Put(list);
            obj = cache.Get(key) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value2);
       

        }
        
		[Test]
		public void TestPut()
		{
			const string key = "key1";
			const string value = "value";

            var cache = _provider.BuildCache("nunit", new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);
			Assert.IsNotNull(cache, "no cache returned");

			Assert.IsNull(cache.Get(key), "cache returned an item we didn't add !?!");

			cache.Put(new CachePutParameters(null, key, value) );
			Thread.Sleep(1000);
			var item = cache.Get(key);
			Assert.IsNotNull(item);
			Assert.AreEqual(value, item, "didn't return the item we added");
		}

		[Test]
		public void TestRegions()
		{
			const string key = "key";
            var cache1 = _provider.BuildCache("nunit1", new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);
            var cache2 = _provider.BuildCache("nunit2", new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);
			const string s1 = "test1";
			const string s2 = "test2";
			cache1.Put(new CachePutParameters(null, key, s1) );
			cache2.Put(new CachePutParameters(null, key, s2) );
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

            var cache = _provider.BuildCache("nunit", new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);
			Assert.IsNotNull(cache, "no cache returned");

			// add the item
			cache.Put(new CachePutParameters(null, key, value));
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