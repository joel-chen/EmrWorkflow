using Amazon.ElasticMapReduce.Model;

namespace EmrWorkflow.Run.Model
{
    public class EmrActivityInfo
    {
        public JobFlowDetail JobFlowDetail { get; set; }

        public EmrActivityState CurrentState { get; set; }
    }
}
