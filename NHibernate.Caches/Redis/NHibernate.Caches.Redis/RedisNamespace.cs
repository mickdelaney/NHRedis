using System;
using System.Collections.Generic;
using System.Text;



namespace NHibernate.Caches.Redis
{
    public class RedisNamespace
    {

        private const string SeparatorOuter = "#";
        private const string SeparatorInner = "?";

        //#?#
        private const string NamespaceSeparator = SeparatorOuter + SeparatorInner + SeparatorOuter;

        //?#
        public const string Uniqueifier = SeparatorInner + SeparatorOuter;


        //??
        private const string Sanitizer = SeparatorInner + SeparatorInner;

        // strings that only have odd-numbered runs SeparatorInner characters in them,
        // and do not end with SeparatorOuter SeparatorInner,
        // are valid, reserved names

        // namespace generation - generation changes namespace is slated for garbage collection
        private int _namespaceGeneration = -1;

        // key for namespace generation
        private readonly string _namespaceGenerationKey;

        //sanitized name for namespace (includes namespace generation)
        private readonly string _namespacePrefix;

        //reserved, unique name for meta entries for this namespace
        private readonly string _namespaceReservedName;

        // key for set of all global keys in this namespace
        private readonly string _globalKeysKey;

        // key for list keys slated for garbage collection
        // (having a single uniqueifier guarantees uniqueness for this key)
        public static string NamespacesGarbageKey = Uniqueifier + "NHREDIS_NAMESPACES_GARBAGE";

        private int uniqueCount = 1;


        public RedisNamespace(string name)
        {
            _namespacePrefix = Sanitize(name);

            _namespaceReservedName = MakeUnique(_namespacePrefix);

            _globalKeysKey = _namespaceReservedName;

            //get generation
            _namespaceGenerationKey = _namespaceReservedName + "_" + "generation";

        }



        public int GetGeneration()
        {
            return _namespaceGeneration;
        }
        public void SetGeneration(int generation)
        {
             _namespaceGeneration = generation;
        }
        public void IncrementGeneration()
        {
            _namespaceGeneration++;
        }

        public string GetGenerationKey()
        {
            return _namespaceGenerationKey;
        }

        public string GetGlobalKeysKey()
        {
            return _globalKeysKey;
        }

        public string GlobalKey(object key)
        {
            var rc = Sanitize(key);
            if (_namespacePrefix != null && !_namespacePrefix.Equals(""))
                rc = _namespacePrefix + "_" + _namespaceGeneration.ToString() + NamespaceSeparator + rc;
            return rc;
        }
        public string GlobalLockKey(object key)
        {
            return Uniqueifier + GlobalKey(key);
        }
        private static string Sanitize(string dirtyString)
        {
            return dirtyString == null ? null : dirtyString.Replace(SeparatorInner, Sanitizer);
        }

        private static string Sanitize(object dirtyString)
        {
            return Sanitize(dirtyString.ToString());
        }
        private string MakeUnique(string myString)
        {
            for (int i = 0; i < uniqueCount; ++i)
                myString = Uniqueifier + myString;
            uniqueCount++;
            return myString;

        }
    }
}
