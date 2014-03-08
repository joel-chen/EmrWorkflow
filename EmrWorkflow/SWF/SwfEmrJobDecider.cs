using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Model;
using EmrWorkflow.SWF.Model;
using EmrWorkflow.Utils;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrPlusSwf
{
    public class SwfEmrJobDecider : TimerWorkerBase
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        public SwfEmrJobDecider(IEmrJobLogger emrJobLogger, IEmrJobStateChecker emrJobStateChecker, IAmazonElasticMapReduce emrClient, IAmazonSimpleWorkflow swfClient)
        {
            this.EmrJobLogger = emrJobLogger;
            this.EmrJobStateChecker = emrJobStateChecker;
            this.EmrClient = emrClient;
            this.SwfClient = swfClient;
        }

        /// <summary>
        /// Object to log information about the EMR Job
        /// </summary>
        public IEmrJobLogger EmrJobLogger { get; set; }

        /// <summary>
        /// Object to check the current state of the EMR Job
        /// </summary>
        public IEmrJobStateChecker EmrJobStateChecker { get; set; }

        /// <summary>
        /// EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public IAmazonElasticMapReduce EmrClient { get; set; }

        /// <summary>
        /// SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        protected async override void DoWorkSafe()
        {
            DecisionTask task = await this.Poll();
            if (!string.IsNullOrEmpty(task.TaskToken))
            {
                //Create the next set of decision based on the current state of the EMR Job
                List<Decision> decisions = await this.Decide();

                //Complete the task with the new set of decisions
                //CompleteTask(task.TaskToken, decisions);
            }
        }

        private async Task<DecisionTask> Poll()
        {
            this.EmrJobLogger.PrintInfo("Polling for decision task ...");
            PollForDecisionTaskRequest request = new PollForDecisionTaskRequest()
            {
                Domain = Constants.EmrJobDomain,
                TaskList = new TaskList()
                {
                    Name = Constants.EmrJobTasksList
                }
            };

            PollForDecisionTaskResponse response = await this.SwfClient.PollForDecisionTaskAsync(request);
            return response.DecisionTask;
        }

        private async Task<List<Decision>> Decide()
        {
            this.EmrJobLogger.PrintCheckingStatus();
            //TODO: implement reading jobflowId from task
            EmrActivityInfo activityInfo = await this.EmrJobStateChecker.CheckAsync(this.EmrClient, ""/*this.JobFlowId*/);

            List<Decision> decisions = new List<Decision>();

            if (activityInfo.CurrentState == EmrActivityState.Running)
            {
                this.EmrJobLogger.PrintJobInfo(activityInfo);
            }
            else
            {
                if (activityInfo.CurrentState == EmrActivityState.Failed)
                {
                    this.EmrJobLogger.PrintError(activityInfo);
                    //TODO: add a decision to start next activity for the failed flow
                }

                //TODO: add a decision to start next activity
            }

            return decisions;
        }
    }
}
