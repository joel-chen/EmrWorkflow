using System;

namespace EmrWorkflow.RequestBuilders
{
    /// <summary>
    /// Interface of an object to keep the settings for the EMR-Request's building process.
    /// It provides a functionality to replace placeholders in a text based on the values stored in the settings.
    /// </summary>
    public interface IBuilderSettings
    {
        /// <summary>
        /// Get setting's value by key
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns>Value</returns>
        string Get(String key);

        /// <summary>
        /// Replace placeholders in a text using current settings
        /// </summary>
        /// <param name="text">Text with placeholders</param>
        /// <returns>Text with replaced placeholders</returns>
        string FillPlaceHolders(string text);
    }
}
