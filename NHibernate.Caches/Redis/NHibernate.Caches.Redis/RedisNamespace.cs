using System;
using System.Collections.Generic;
using System.Text;



namespace NHibernate.Caches.Redis
{
    public class RedisNamespace
    {

        private const string UniqueCharacter = "?";

        //make reserved keys unique by tacking N of these to the beginning of the string
        private const string ReservedTag = "@" + UniqueCharacter + "@";

        //unique separator between namespace and key
        private const string NamespaceKeySeparator = "#" + UniqueCharacter + "#";

        //make non-static keys unique by tacking on N of these to the end of the string
        public const string KeyTag = "%" + UniqueCharacter + "%";

        public const string NamespaceTag = "!" + UniqueCharacter + "!";

        //remove any odd numbered runs of the UniqueCharacter character
        private const string Sanitizer = UniqueCharacter + UniqueCharacter;

        // namespace generation - generation changes namespace is slated for garbage collection
        private long _namespaceGeneration = -1;

        // key for namespace generation
        private readonly string _namespaceGenerationKey;

        //sanitized name for namespace (includes namespace generation)
        private readonly string _namespacePrefix;

        //reserved, unique name for meta entries for this namespace
        private readonly string _namespaceReservedName;

        // key for set of all global keys in this namespace
        private readonly string _globalKeysKey;

        // key for list of keys slated for garbage collection
        // (having two flanking uniqueifiers guarantees uniqueness for this key)
        public const string NamespacesGarbageKey = ReservedTag + "NHREDIS_NAMESPACES_GARBAGE";

        public const int NumTagsForKey = 0;
        public const int NumTagsForLockKey = 1;

        public RedisNamespace(string name)
        {
            _namespacePrefix = Sanitize(name);

            _namespaceReservedName = NamespaceTag + _namespacePrefix;

            _globalKeysKey = _namespaceReservedName;

            //get generation
            _namespaceGenerationKey = _namespaceReservedName + "_" + "generation";

        }



        public long GetGeneration()
        {
            return _namespaceGeneration;
        }
        public void SetGeneration(long generation)
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

        public string GlobalKey(object key, int numUniquePrefixes)
        {
            var rc = Sanitize(key);
            if (_namespacePrefix != null && !_namespacePrefix.Equals(""))
                rc = _namespacePrefix + "_" + _namespaceGeneration.ToString() + NamespaceKeySeparator + rc;
            for (int i = 0; i < numUniquePrefixes; ++i)
                rc += KeyTag;
            return rc;
        }
        private static string Sanitize(string dirtyString)
        {
            return dirtyString == null ? null : dirtyString.Replace(UniqueCharacter, Sanitizer);
        }

        private static string Sanitize(object dirtyString)
        {
            return Sanitize(dirtyString.ToString());
        }
    }
}
