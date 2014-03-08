using EmrWorkflow.Run.Model;

namespace EmrWorkflow.SWF.Model
{
    public class SwfEmrActivity
    {
        /// <summary>
        /// Name of the EMR Activity
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Path to the XML-file that describes activity
        /// </summary>
        public string FilePath { get; set; }

        /// <summary>
        /// EMR Activity type
        /// </summary>
        public EmrActivityType Type { get; set; }
    }
}
