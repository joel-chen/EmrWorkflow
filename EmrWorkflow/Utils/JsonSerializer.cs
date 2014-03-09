using System.Text;
using System.Web.Script.Serialization;

namespace EmrWorkflow.Utils
{
    class JsonSerializer
    {
        public static T Deserialize<T>(string json)
        {
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            T inputs = serializer.Deserialize<T>(json);
            return inputs;
        }

        public static string Serialize<T>(T inputs)
        {
            JavaScriptSerializer serializer = new JavaScriptSerializer();
            StringBuilder builder = new StringBuilder();
            serializer.Serialize(inputs, builder);
            return builder.ToString();
        }
    }
}
