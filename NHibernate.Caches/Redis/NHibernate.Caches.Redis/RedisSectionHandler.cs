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
using System.Configuration;
using System.Xml;

namespace NHibernate.Caches.Redis
{
	/// <summary>
	/// config file provider
	/// </summary>
	public class RedisSectionHandler : IConfigurationSectionHandler
	{
		private static readonly IInternalLogger log = LoggerProvider.LoggerFor((typeof(RedisSectionHandler)));

		#region IConfigurationSectionHandler Members

		/// <summary>
		/// parse the config section
		/// </summary>
		/// <param name="parent"></param>
		/// <param name="configContext"></param>
		/// <param name="section"></param>
		/// <returns>an array of <see cref="MemCacheConfig"/> objects</returns>
		public object Create(object parent, object configContext, XmlNode section)
		{
            RedisConfig config = null;
            if (section != null)
            {
                XmlNodeList nodes = section.SelectNodes("redis");
                XmlNode node = nodes[0];
  
                    XmlAttribute h = node.Attributes["host"];
                    XmlAttribute p = node.Attributes["port"];
                    XmlAttribute maxReadPoolSize = node.Attributes["maxReadPoolSize"];
                    XmlAttribute maxWritePoolSize = node.Attributes["maxWritePoolSize"];
                    if (h == null || p == null)
                    {
                        if (log.IsWarnEnabled)
                        {
                            log.Warn("incomplete node found - each redis element must have a 'host' and a 'port' attribute.");
                        }
                    } else 
                    {
                        string host = h.Value;
                        config = new RedisConfig(host, parseInt(p));
                        if (maxReadPoolSize != null)
                            config.MaxReadPoolSize = parseInt(maxReadPoolSize);
                        if (maxWritePoolSize != null)
                            config.MaxWritePoolSize = parseInt(maxWritePoolSize);

                    }
            }
            return config;
		}

        private int parseInt(XmlAttribute attr)
        {
            return ((string.IsNullOrEmpty(attr.Value)) ? 0 : Convert.ToInt32(attr.Value));
        }

		#endregion
	}
}