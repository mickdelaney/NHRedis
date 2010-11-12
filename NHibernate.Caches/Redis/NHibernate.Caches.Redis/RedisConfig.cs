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
using System.Collections.Generic;
using System.Configuration;
namespace NHibernate.Caches.Redis
{
    public class RedisConfig
    {
        private string host_;
        private int port_;
        // Create a configuration section.
        public RedisConfig()
        {
            host_ = "localhost";
            port_ = 6137;
        }

        public RedisConfig(string host, int port)
        {
            host_ = host;
            port_ = port;
        }

        public string Host
        {
            get
            {
                return host_;
            }
            set
            {
                host_ = value;
            }
        }
        public int Port
        {
            get
            {
                return port_;
            }
            set
            {
                port_ = value;
            }
        }
    }

}
