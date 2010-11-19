using System;
using System.Collections.Generic;
using System.Linq;
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

        // namespace generation - generation changes when namespace is deleted
        private int namespaceGeneration = -1;

        // key for namespace generation
        private readonly string namespaceGenerationKey;

        //sanitized name for namespace (includes namespace generation)
        private readonly string namespacePrefix;

        //reserved, unique name for meta entries for this namespace
        private readonly string namespaceReservedName;

        // key for set of namespace keys
        private readonly string namespaceKeysKey;

        public static readonly string namespacesToCleanKey = separatorInner + "REDIS_NAMESPACES_TO_CLEAN" + separatorInner;


        public RedisNamespace(string name)
        {
            namespacePrefix = sanitize(name);

            //no sanitized string can have an odd-length substring of separatorInner characters
            namespaceReservedName = separatorInner + namespacePrefix;

            namespaceKeysKey = namespaceReservedName;

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

        public string getNamespaceKeysKey()
        {
            return namespaceKeysKey;
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
