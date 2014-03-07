using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.Run;
using System;

namespace EmrPlusSwf
{
    public class EmrDecider : EmrWorkerBase
    {
        /// <summary>
        /// Instantiated SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        protected override void DoWorkSafe()
        {
            throw new System.NotImplementedException();
        }

        DecisionTask Poll()
        {
            EmrJobLogger.PrintInfo("Polling for decision task ...");
            PollForDecisionTaskRequest request = new PollForDecisionTaskRequest()
            {
                Domain = Constants.EmrJobDomain,
                TaskList = new TaskList()
                {
                    Name = Constants.EmrJobTasksList
                }
            };

            PollForDecisionTaskResponse response = this.SwfClient.PollForDecisionTask(request);
            return response.DecisionTask;
        }
    }
}
