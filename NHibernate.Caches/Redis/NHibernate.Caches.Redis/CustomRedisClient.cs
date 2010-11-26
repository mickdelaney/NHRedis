using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections;
using System.IO;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClient : RedisClient
    {
        private MemoryStream _memoryStream = new MemoryStream(1024);
        private BinaryFormatter _bf = new BinaryFormatter();

        public CustomRedisClient(string host, int port)
			: base(host, port)
		{
		}

        // Serialize object to buffer
        public  byte[] Serialize(object value)
        {
            var dictEntry = new DictionaryEntry(null, value);
            _memoryStream.Seek(0, 0);
            _bf.Serialize(_memoryStream, dictEntry);
            return _memoryStream.GetBuffer();
        }
        // Deserialize buffer to object
        public  object Deserialize(byte[] someBytes)
        {
            
            _memoryStream.Seek(0, 0);
            _memoryStream.Write(someBytes, 0, someBytes.Length);
            _memoryStream.Seek(0, 0);
            var de = (DictionaryEntry)_bf.Deserialize(_memoryStream);
            return de.Value;
        }

    }
}
