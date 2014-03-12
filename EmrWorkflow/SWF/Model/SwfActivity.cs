using EmrWorkflow.Run.Model;

namespace EmrWorkflow.SWF.Model
{
    class SwfActivity
    {
        /// <summary>
        /// Name of the EMR Activity
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Path of the EMR Activity
        /// </summary>
        public string Path { get; set; }

        /// <summary>
        /// EMR Activity type
        /// </summary>
        public EmrActivityType Type { get; set; }

        /// <summary>
        /// Current jobflow's id
        /// </summary>
        public string JobFlowId { get; set; }
    }
}
