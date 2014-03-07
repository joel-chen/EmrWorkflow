using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.Run;

namespace EmrPlusSwf
{
    public class EmrDecider : EmrWorkerBase
    {
        protected override void DoWorkSafe()
        {
            throw new System.NotImplementedException();
        }

        DecisionTask Poll()
        {
            this._console.WriteLine("Polling for decision task ...");
            PollForDecisionTaskRequest request = new PollForDecisionTaskRequest()
            {
                Domain = Constants.ImageProcessingDomain,
                TaskList = new TaskList()
                {
                    Name = Constants.ImageProcessingTaskList
                }
            };

            PollForDecisionTaskResponse response = _swfClient.PollForDecisionTask(request);
            return response.DecisionTask;
        }
    }
}
