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
using NHibernate.Cache.Entry;
using NHibernate.Cache.Query;
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
		public virtual void FixtureSetup()
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
        public void TestHSet()
        {
            var key = "key";
            var field = "foo";
            var value = new LiveQueryCacheEntry("value",1,null);
            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);
            Assert.IsFalse(cache.HDel(key, field));
            cache.HSet(key, field, value);
            Assert.IsTrue(cache.HDel(key, field));
        }

        [Test]
        public void TestHSetMultiple()
        {
            var key = "key";

            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);
            var members = cache.HGetAll(key);
            for (int i = 0; i < members.Count; i+=2 )
            {
                cache.HDel(key, members[i].ToString());
            }

            var keyValues = new Dictionary<object, LiveQueryCacheEntry>();
            keyValues["field1"] = new LiveQueryCacheEntry("value1", 1, null);
            keyValues["field2"] = new LiveQueryCacheEntry("value2", 1, null);
            foreach (var entry in keyValues)
            {
                Assert.IsFalse(cache.HDel(key, entry.Key));
            }

            cache.HSet(key, keyValues);
            members = cache.HGetAll(key);
            foreach (var entry in keyValues)
            {
                Assert.IsTrue(cache.HDel(key, entry.Key));
            }
        }

        
        [Test]
        public void TestHGetAll()
        {
            var cache = _provider.BuildLiveQueryCache(typeof(String).FullName, _props);

            var key = "keykey";
            var members = cache.HGetAll(key);
            foreach (var member in members)
            {
                cache.HDel(key, member.Key);
            }

            var keyValues = new Dictionary<object, LiveQueryCacheEntry>();
            keyValues["field1"] = new LiveQueryCacheEntry("value1", 1, null);
            keyValues["field2"] = new LiveQueryCacheEntry("value2", 1, null);
            cache.HSet(key, keyValues);

            members = cache.HGetAll(key);

            var fields = new ArrayList()
                                    {
                                        "field1", "field2"
                                    };
            var vals = new ArrayList()
                                    {
                                        "value1", "value2"
                                    };
            foreach (var member in members)
            {
                Assert.IsTrue(fields.Contains(member.Key));
                Assert.IsTrue(vals.Contains(member.Value.Value)); 
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
            const string key1 = "key1";

            var comparer = new SimpleComparer();
            var cache = _provider.BuildCache(typeof(String).FullName, new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);

            int version1 = 1;
            string value1 = "value1";

            int version2 = 2;
            string value2 = "value2";

            int version3 = 3;
            string value3 = "value3";

            cache.Remove(key1);


            //check if object is cached correctly
            var versionParams =
	        new CacheVersionedPutParameters()
	            {
	                Key = key1,
	                Value = value1,
	                Version = version1,
	                VersionComparer = comparer
	            };
	        var list = new List<CacheVersionedPutParameters> {versionParams};
            cache.Put(list);
            var obj = cache.Get(key1) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value1);

	        versionParams.Value = value2;
	        versionParams.Version = version2;
            // check that object changes with next version
            cache.Put(list);
            obj = cache.Get(key1) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value2);

            // check that older version does not change cache
            versionParams.Value = value3;
            versionParams.Version = version1;
            cache.Put(list);
            obj = cache.Get(key1) as LockableCachedItem;
            Assert.AreEqual(obj.Value, value2);


        }

        [Test]
        public void TestVersionedPutMultiple()
        {
            var comparer = new SimpleComparer();
            var cache = _provider.BuildCache(typeof(String).FullName, new TestInMemoryQueryProvider(), CacheFactory.ReadWriteCow, _props);

            // test put on list
            var keys = new[] { "key1", "key2", "key3" };
            var values = new[] { "value1", "value2", "value3" };
            var versions = new[] { 1, 2, 3 };

            foreach (var k in keys)
                cache.Remove(k);
            var list = new List<CacheVersionedPutParameters>(3);
            for (int i = 0; i < 3; ++i)
                list.Add(new CacheVersionedPutParameters()
                {
                    Key = keys[i],
                    Value = values[i],
                    Version = versions[i],
                    VersionComparer = comparer
                });

            cache.Put(list);

            for (int i = 0; i < 3; ++i)
            {
                var res = cache.Get(keys[i]) as LockableCachedItem;
                Assert.AreEqual(res.Value, values[i]);
                Assert.AreEqual(res.Version, versions[i]);
            }
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