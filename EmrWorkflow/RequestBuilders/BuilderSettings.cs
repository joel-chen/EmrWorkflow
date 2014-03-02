using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace EmrWorkflow.RequestBuilders
{
    public class BuilderSettings
    {
        /// <summary>
        /// Reserved key.
        /// Used to identify the current job.
        /// Populated either automatically by the <see cref="EmrWorkflow.Run.Strategies.StartJobStrategy"/>
        /// or manually by a user.
        /// </summary>
        public const String JobFlowId = "jobFlowId";

        /// <summary>
        /// Reserved key.
        /// Used to specify the current HBase version.
        /// Populated either automatically from the <see cref="EmrWorkflow.Model.Configs.HBaseConfig"/>
        /// or manually by a user.
        /// </summary>
        public const String HBaseJarPath = "hBaseJarPath";

        private Regex placeHolderRegex;
        private Dictionary<string, string> settings;

        public BuilderSettings()
        {
            this.placeHolderRegex = new Regex(@"{\w*}");
            this.settings = new Dictionary<string, string>();
        }

        public void Put(String key, String value)
        {
            this.settings.Add(key, value);
        }

        public String Get(String key)
        {
            if (!this.settings.ContainsKey(key))
                return null;

            return this.settings[key];
        }

        public string FillPlaceHolders(string text)
        {
            if (String.IsNullOrEmpty(text))
                return text;

            return this.placeHolderRegex.Replace(text, match => 
            {
                string key = match.Value.Substring(1, match.Value.Length - 2);
                return (this.Get(key) ?? match.Value);
            });
        }
    }
}
