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
                XmlAttribute h=null, p=null, maxReadPoolSize=null, maxWritePoolSize=null;
                if (node != null)
                {
                    h = node.Attributes["host"];
                    p = node.Attributes["port"];
                    maxReadPoolSize = node.Attributes["maxReadPoolSize"];
                    maxWritePoolSize = node.Attributes["maxWritePoolSize"];
                }
                var host = (h != null && h.Value != null) ? h.Value :  "localhost";
                var port =  p != null ? parseInt(p) : 6379;
		        if (h == null || p == null)
                {
                    if (log.IsWarnEnabled)
                    {
                        log.Warn("incomplete node found - each redis element must have a 'host' and a 'port' attribute. Using default value(s)");
                    }
                  
                } 
                config = new RedisConfig(host, port);
                if (maxReadPoolSize != null)
                    config.MaxReadPoolSize = parseInt(maxReadPoolSize);
                if (maxWritePoolSize != null)
                    config.MaxWritePoolSize = parseInt(maxWritePoolSize);
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