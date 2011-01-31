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
using log4net.Config;
using NHibernate.Cache;
using NUnit.Framework;

namespace NHibernate.Caches.Redis.Tests
{
	public class RedisProviderFixture
	{
		private Dictionary<string, string> _props;
		private ICacheProvider _provider;

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			XmlConfigurator.Configure();
			_props = new Dictionary<string, string>();
			_provider = new RedisProvider();
			_provider.Start(_props);
		}

		[TestFixtureTearDown]
		public void Stop()
		{
			_provider.Stop();
		}

		[Test]
		public void TestBuildCacheFromConfig()
		{
			var cache = _provider.BuildCache("foo", null, null, null);
			Assert.IsNotNull(cache, "pre-configured cache not found");
		}

		[Test]
		public void TestBuildCacheNullNull()
		{
			var cache = _provider.BuildCache(null, null, null, null);
			Assert.IsNotNull(cache, "no cache returned");
		}

		[Test]
		public void TestBuildCacheStringICollection()
		{
			var cache = _provider.BuildCache("another_region", null, null,_props);
			Assert.IsNotNull(cache, "no cache returned");
		}

		[Test]
		public void TestBuildCacheStringNull()
		{
			var cache = _provider.BuildCache("a_region", null, null, null);
			Assert.IsNotNull(cache, "no cache returned");
		}

		[Test]
		public void TestNextTimestamp()
		{
			var ts = _provider.NextTimestamp();
			Assert.IsNotNull(ts, "no timestamp returned");
		}
	}
}