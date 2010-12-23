using System.Collections.Generic;
using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;
using System.Collections;
using System.IO;
using System;

namespace NHibernate.Caches.Redis
{
    public class CustomRedisClient : RedisClient
    {
        private MemoryStream _memoryStream = new MemoryStream();
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

        public long FetchGeneration(string generationKey)
        {
            var val = GetValue(generationKey);
            return (val == null) ? Incr(generationKey) : Convert.ToInt64(val);
        }

        public List<byte[]> Serialize(object[] values)
        {
            var rc = new List<byte[]>();
            foreach (var value in values)
            {
                var bytes = Serialize(value);
                if (bytes != null)
                    rc.Add(bytes);
            }
            return rc;
        }


        // Serialize object to buffer
        public  byte[] Serialize(object value)
        {
            if (value == null)
                return null;
            var dictEntry = new DictionaryEntry(null, value);
            _memoryStream.Seek(0, 0);
            _bf.Serialize(_memoryStream, dictEntry);
            return _memoryStream.GetBuffer();
        }

        // Deserialize buffer to object
        public  object Deserialize(byte[] someBytes)
        {         
            if (someBytes == null)
                return null;
            _memoryStream.Seek(0, 0);
            _memoryStream.Write(someBytes, 0, someBytes.Length);
            _memoryStream.Seek(0, 0);
            var de = (DictionaryEntry)_bf.Deserialize(_memoryStream);
            return de.Value;
        }
        public IList Deserialize(byte[][] byteArray)
        {
            IList rc = new ArrayList();
            foreach (var someBytes in byteArray)
            {
                var obj = Deserialize(someBytes);
                if (obj != null)
                    rc.Add(obj);
            }
            return rc;
        }
    }
}
