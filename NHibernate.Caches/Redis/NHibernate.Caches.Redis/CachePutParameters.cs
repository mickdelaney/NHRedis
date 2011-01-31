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

namespace NHibernate.Caches.Redis
 {
     public class CachePutParameters
     {
         public CachePutParameters()
         {

         }
         public CachePutParameters(object hydratedObject, object key, object value)
         {
             HydratedObject = hydratedObject;
             Key = key;
             Value = value;
         }
         public object HydratedObject
         {
             get;
             set;
         }
         public object Key
         {
             get;
             set;
         }
         public object Value
         {
             get;
             set;
         }
         public override string ToString()
         {
             return "CachePutParameters: {key= " + Key +
                    ", value= " + Value +
                    ", hydrated object= " + HydratedObject +
                    "}";
         }

     }
 }
