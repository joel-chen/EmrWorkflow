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
                ProcessActivityResult processResult = await ProcessTask(activityTask.Input);

                if (string.IsNullOrEmpty(processResult.ErrorMessage))
                    await CompleteTask(activityTask.TaskToken, processResult.Output);
                else
                    await FailTask(activityTask.TaskToken, activityTask.Input, processResult.ErrorMessage);
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

        private async Task<ProcessActivityResult> ProcessTask(string input)
        {
            ProcessActivityResult result = new ProcessActivityResult();

            try
            {
                SwfEmrActivity swfActivity = JsonSerializer.Deserialize<SwfEmrActivity>(input);
                SwfSingleEmrActivityIterator singleEmrActivityIterator = new SwfSingleEmrActivityIterator(swfActivity);

                using (EmrActivitiesRunner emrRunner = new EmrActivitiesRunner(this.EmrJobLogger, this.EmrJobStateChecker, this.EmrClient, this.Settings, singleEmrActivityIterator))
                {
                    emrRunner.JobFlowId = swfActivity.JobFlowId; //set JobFlowId for the current activity
                    bool emrJobOk = await emrRunner.Start();

                    if (emrJobOk)
                    {
                        swfActivity.JobFlowId = emrRunner.JobFlowId; //read JobFlowId in case it was changed (for example, during starting a new EMR job)
                        result.Output = swfActivity;
                    }
                    else
                    {
                        result.ErrorMessage = emrRunner.ErrorMessage;
                    }
                }
            }
            catch (Exception ex)
            {
                result.ErrorMessage = ex.Message;
            }

            return result;
        }

        private async Task FailTask(String taskToken, string input, string errorMessage)
        {
            RespondActivityTaskFailedRequest request = new RespondActivityTaskFailedRequest()
            {
                TaskToken = taskToken,
                Reason = SwfResources.Info_EmrJobFailed,
                Details = errorMessage
            };

            RespondActivityTaskFailedResponse response = await this.SwfClient.RespondActivityTaskFailedAsync(request);
            this.EmrJobLogger.PrintInfo(string.Format(SwfResources.Info_SwfActivityFailedTemplate, input));
        }

        private async Task CompleteTask(String taskToken, SwfEmrActivity swfActivity)
        {
            RespondActivityTaskCompletedRequest request = new RespondActivityTaskCompletedRequest()
            {
                Result = JsonSerializer.Serialize<SwfEmrActivity>(swfActivity),
                TaskToken = taskToken
            };

            RespondActivityTaskCompletedResponse response = await this.SwfClient.RespondActivityTaskCompletedAsync(request);
            this.EmrJobLogger.PrintInfo(string.Format("Activity task \"{0}\" completed. JobFlowId: {1}." , swfActivity.Name, swfActivity.JobFlowId));
        }

        protected override bool WorkerResult
        {
            get { throw new NotImplementedException(); }
        }
    }
}
