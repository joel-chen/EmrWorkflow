using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Model;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace EmrPlusSwf
{
    public class SwfEmrJobDecider : EmrJobManagerBase
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public SwfEmrJobDecider(IEmrJobLogger emrJobLogger, IAmazonSimpleWorkflow swfClient, IAmazonElasticMapReduce emrClient, IEmrJobStateChecker emrJobStateChecker, BuilderSettings settings, EmrActivitiesEnumerator emrActivitiesEnumerator)
            : base(emrJobLogger, emrClient, emrJobStateChecker, settings)
        {
            this.SwfClient = swfClient;
        }

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
                CompleteTask(task.TaskToken, decisions);
            }
        }

        private async Task<DecisionTask> Poll()
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

            PollForDecisionTaskResponse response = await this.SwfClient.PollForDecisionTaskAsync(request);
            return response.DecisionTask;
        }

        private async Task<List<Decision>> Decide()
        {
            EmrActivityInfo activityInfo = await this.CheckJobStateAsync();
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
