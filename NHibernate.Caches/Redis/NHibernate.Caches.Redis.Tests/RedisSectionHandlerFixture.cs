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

using System.Xml;
using NUnit.Framework;


namespace NHibernate.Caches.Redis.Tests
{
	[TestFixture]
	public class RedisSectionHandlerFixture
	{
		#region Setup/Teardown

		[SetUp]
		public void Init()
		{
			handler = new RedisSectionHandler();
			var doc = new XmlDocument();
			doc.LoadXml(xml);
			section = doc.DocumentElement;
		}

		#endregion

		private RedisSectionHandler handler;
		private XmlNode section;
        private string xml = "<redis><redis host=\"142.223.144.62\" port=\"6379\" /></redis>";

		[Test]
		public void TestGetConfigFromFile()
		{
			object result = handler.Create(null, null, section);
			Assert.IsNotNull(result);
			Assert.IsTrue(result is RedisConfig);
		}
/*
		[Test]
		public void TestGetConfigNullSection()
		{
			section = new XmlDocument();
			object result = handler.Create(null, null, section);
			Assert.IsNotNull(result);
            Assert.IsTrue(result is RedisConfig);
		}
*/
    }
}