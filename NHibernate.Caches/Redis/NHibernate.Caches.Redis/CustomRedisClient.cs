using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections;
using System.IO;
using System;

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

        public void Lock(string lockKey)
        {
            var temp = new byte[1];
            int wasSet = SetNX(lockKey, temp);
            while (wasSet == 0)
            {
                System.Threading.Thread.Sleep(100);
                wasSet = SetNX(lockKey, temp);
            }
        }

        public void Unlock(string lockKey)
        {
           Del(lockKey);           
        }

        public int FetchGeneration(string generationKey)
        {
            int rc = 0;
            string val = GetValue(generationKey);
            if (val == null)
            {
                Set<int>(generationKey, 0);
            }
            else
            {
                rc = Convert.ToInt32(val);
            }
            return rc;
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
