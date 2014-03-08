using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.SWF.Model;
using EmrWorkflow.Utils;
using System;
using System.Threading.Tasks;

namespace EmrWorkflow.SWF
{
    public class SwfEmrActivityProcessor : TimerWorkerBase
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        public SwfEmrActivityProcessor(IEmrJobLogger emrJobLogger, IBuilderSettings settings, IAmazonElasticMapReduce emrClient, IAmazonSimpleWorkflow swfClient)
        {
            this.EmrJobLogger = emrJobLogger;
            this.Settings = settings;
            this.EmrClient = emrClient;
            this.SwfClient = swfClient;
            this.ActivitiesFactory = new EmrActivitiesFactory();
        }

        /// <summary>
        /// Object to log information about the EMR Job
        /// </summary>
        public IEmrJobLogger EmrJobLogger { get; set; }

        /// <summary>
        /// Settings to replace placeholders
        /// </summary>
        public IBuilderSettings Settings { get; set; }

        /// <summary>
        /// EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public IAmazonElasticMapReduce EmrClient { get; set; }

        /// <summary>
        /// SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        public EmrActivitiesFactory ActivitiesFactory { get; set; }

        protected async override void DoWorkSafe()
        {
            ActivityTask activityTask = await this.Poll();
            if (!String.IsNullOrEmpty(activityTask.TaskToken))
            {
                SwfEmrActivity activityState = ProcessTask(activityTask.Input);
                //CompleteTask(activityTask.TaskToken, activityState);
            }
        }

        private async Task<ActivityTask> Poll()
        {
            EmrJobLogger.PrintInfo("Polling for activity task ...");
            PollForActivityTaskRequest request = new PollForActivityTaskRequest()
            {
                Domain = Constants.EmrJobDomain,
                TaskList = new TaskList()
                {
                    Name = ""//Constants.ImageProcessingActivityTaskList
                }
            };

            PollForActivityTaskResponse response = await this.SwfClient.PollForActivityTaskAsync(request);
            return response.ActivityTask;
        }

        private SwfEmrActivity ProcessTask(string input)
        {
            /*
            SwfEmrActivity activity = Utils.DeserializeFromJSON<ActivityState>(input);
            EmrActivityStrategy strategy = this.ActivitiesFactory.CreateStrategy(activity);

            this.EmrJobLogger.PrintAddingNewActivity(strategy);
            bool await strategy.PushAsync(this);

            return activityState;*/
            return null;
        }
    }
}
