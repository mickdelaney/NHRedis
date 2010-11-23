using System;
using System.Collections.Generic;
using System.Text;



namespace NHibernate.Caches.Redis
{
    public class RedisNamespace
    {

        private static readonly string separatorOuter = "#";
        private static readonly string separatorInner = "?";

        //#?#
        private static readonly string namespaceSeparator = separatorOuter + separatorInner + separatorOuter;

        //??
        private static readonly string sanitizer = separatorInner + separatorInner;

        // strings that have only single separatorInner characters in them,
        // and do not end with separatorOuter separatorInner,
        // are valid, reserved names

        // namespace generation - generation changes namespace is slated for garbage collection
        private int namespaceGeneration = -1;

        // key for namespace generation
        private readonly string namespaceGenerationKey;

        //sanitized name for namespace (includes namespace generation)
        private readonly string namespacePrefix;

        //reserved, unique name for meta entries for this namespace
        private readonly string namespaceReservedName;

        // key for set of all global keys in this namespace
        private readonly string globalKeysKey;

        // key for list keys slated for garbage collection
        // (having two single separatorInner characters guarantees uniqueness for this key)
        public static readonly string namespacesGarbageKey = separatorInner + "REDIS_NAMESPACES_GARBAGE" + separatorInner;


        public RedisNamespace(string name)
        {
            namespacePrefix = sanitize(name);

            //no sanitized string can have an odd-length substring of separatorInner characters
            namespaceReservedName = separatorInner + namespacePrefix;

            globalKeysKey = namespaceReservedName;

            //get generation
            namespaceGenerationKey = namespaceReservedName + "_" + "generation";

        }



        public int getGeneration()
        {
            return namespaceGeneration;
        }
        public void setGeneration(int generation)
        {
             namespaceGeneration = generation;
        }
        public void incrementGeneration()
        {
            namespaceGeneration++;
        }

        public string getGenerationKey()
        {
            return namespaceGenerationKey;
        }

        public string getGlobalKeysKey()
        {
            return globalKeysKey;
        }

        public string globalKey(object key)
        {
            string rc = sanitize(key);
            if (namespacePrefix != null && !namespacePrefix.Equals(""))
                rc = namespacePrefix + "_" + namespaceGeneration.ToString() + namespaceSeparator + rc;
            return rc;
        }
        private string sanitize(string dirtyString)
        {
            if (dirtyString == null)
                return null;
            return dirtyString.Replace(separatorInner, sanitizer);

        }
        private string sanitize(object dirtyString)
        {
            return sanitize(dirtyString.ToString());
        }
    }
}
