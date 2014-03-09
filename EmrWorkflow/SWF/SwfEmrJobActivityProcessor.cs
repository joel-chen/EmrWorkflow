using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using EmrWorkflow.Run.Strategies;
using EmrWorkflow.SWF.Model;
using EmrWorkflow.Utils;
using System;
using System.Threading.Tasks;

namespace EmrWorkflow.SWF
{
    public class SwfEmrJobActivityProcessor : TimerWorkerBase<bool>
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        public SwfEmrJobActivityProcessor(IEmrJobLogger emrJobLogger, IEmrJobStateChecker emrJobStateChecker, IBuilderSettings settings, IAmazonElasticMapReduce emrClient, IAmazonSimpleWorkflow swfClient)
        {
            this.EmrJobLogger = emrJobLogger;
            this.EmrJobStateChecker = emrJobStateChecker;
            this.Settings = settings;
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

        protected async override void DoWorkSafe()
        {
            ActivityTask activityTask = await this.Poll();
            if (!String.IsNullOrEmpty(activityTask.TaskToken))
            {
                SwfEmrActivity activityState = await ProcessTask(activityTask.Input);
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
                    Name = Constants.EmrJobTasksList
                }
            };

            PollForActivityTaskResponse response = await this.SwfClient.PollForActivityTaskAsync(request);
            return response.ActivityTask;
        }

        private async Task<SwfEmrActivity> ProcessTask(string input)
        {
            SwfEmrActivity swfActivity = JsonSerializer.Deserialize<SwfEmrActivity>(input);
            EmrActivitiesIteratorBase activitiesIterator = new SwfEmrActivitiesIterator(swfActivity);

            using (EmrJobRunner emrRunner = new EmrJobRunner(this.EmrJobLogger, this.EmrJobStateChecker, this.EmrClient, this.Settings, activitiesIterator))
            {
                emrRunner.JobFlowId = swfActivity.JobFlowId;
                bool result = await emrRunner.Start();
            }

            return null;
        }

        protected override bool Result
        {
            get { throw new NotImplementedException(); }
        }
    }
}
