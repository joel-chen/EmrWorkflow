using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace EmrWorkflow.RequestBuilders
{
    public class BuilderSettings : IBuilderSettings
    {
        private Regex placeHolderRegex;
        private Dictionary<string, string> settings;

        public BuilderSettings()
        {
            this.placeHolderRegex = new Regex(@"{\w*}");
            this.settings = new Dictionary<string, string>();
        }

        public void Put(string key, string value)
        {
            this.settings.Add(key, value);
        }

        public string Get(String key)
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
