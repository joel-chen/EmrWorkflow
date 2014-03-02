using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public class EmrJobRunner
    {
        private const int timerPeriod = 60000 * 1; //every 1 minute

        /// <summary>
        /// Internal field used for sync access to the <see cref="CheckStatus"/> method called by the timer
        /// </summary>
        private int isBusy;

        /// <summary>
        /// Timer for calling <see cref="CheckStatus"/> method
        /// </summary>
        private Timer threadTimer;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrActivitiesIterator">Iterator through the job flow's activities</param>
        public EmrJobRunner(BuilderSettings settings, AmazonElasticMapReduceClient emrClient, EmrActivitiesIterator emrActivitiesIterator)
        {
            this.isBusy = 0;
            this.threadTimer = new Timer(this.CheckStatus);

            this.Settings = settings;
            this.EmrClient = emrClient;
            this.EmrActivitiesIterator = emrActivitiesIterator;
        }


        /// <summary>
        /// If the job is running
        /// </summary>
        public bool IsRunning
        {
            get { return this.threadTimer != null; }
        }

        /// <summary>
        /// Current job flow's id.
        /// Is set automatically after submitting a new job to EMR.
        /// If the job already exists, should be set manually.
        /// </summary>
        public string JobFlowId
        {
            get { return this.Settings.Get(BuilderSettings.JobFlowId); }
            set
            {
                this.Settings.Put(BuilderSettings.JobFlowId, value);
            }
        }

        /// <summary>
        /// Settings to replace placeholders
        /// </summary>
        public BuilderSettings Settings { get; set; }

        /// <summary>
        /// Instantiated EMR Client to make requests to the Amazon EMR Service
        /// </summary>
        public AmazonElasticMapReduceClient EmrClient { get; set; }

        /// <summary>
        /// Iterator through the job flow's activities
        /// </summary>
        public EmrActivitiesIterator EmrActivitiesIterator { get; set; }

        /// <summary>
        /// Start the job flow
        /// </summary>
        public async void Run()
        {
            if ((await this.PushNextActivity()))
                this.threadTimer.Change(0, timerPeriod);
            else
                this.StopRunning();
        }

        private async void CheckStatus(Object stateInfo)
        {
            if (Interlocked.CompareExchange(ref this.isBusy, 1, 0) != 0)
                return;

            Console.WriteLine("--------------------------");
            EmrJobStateChecker jobStateChecker = new EmrJobStateChecker();
            EmrActivityInfo activityInfo = jobStateChecker.Check(this.EmrClient, this.JobFlowId);

            if (activityInfo.CurrentState == EmrActivityState.Running)
            {
                this.PrintJobInfo(activityInfo);
            }
            else if (activityInfo.CurrentState == EmrActivityState.Failed)
            {
                EmrJobRunner.PrintError(activityInfo);
                this.EmrActivitiesIterator.NotifyJobFailed();
            }

            if (!(await this.PushNextActivity()))
                this.StopRunning();

            Interlocked.Exchange(ref this.isBusy, 0);
        }

        private async Task<bool> PushNextActivity()
        {
            if (!this.EmrActivitiesIterator.MoveNext)
            {
                Console.WriteLine("Done...");
                return false;
            }

            EmrActivityStrategy activity = this.EmrActivitiesIterator.Current;
            Console.WriteLine("Adding activity: " + activity.Name);

            //TODO: probably add a retry cycle
            bool pushResult;
            try
            {
                pushResult = await activity.PushAsync(this);
            }
            catch (Exception ex)
            {
                EmrJobRunner.PrintError("Exception during sending the request: " + ex.Message);
                return false;
            }

            if (!pushResult)
            {
                EmrJobRunner.PrintError("Request failed...");
                return false;
            }

            return true;
        }

        private void StopRunning()
        {
            this.threadTimer.Dispose();
            this.threadTimer = null;
        }

        private static void PrintError(EmrActivityInfo activityInfo)
        {
            String errorMessage = activityInfo.JobFlowDetail.ExecutionStatusDetail.LastStateChangeReason;
            EmrJobRunner.PrintError(String.Format(Resources.FailToRunJobTemplate, errorMessage));
        }

        private static void PrintError(string errorMessage)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ResetColor();
        }

        private void PrintJobInfo(EmrActivityInfo activityInfo)
        {
            Console.WriteLine(String.Format(Resources.JobCurrentStateTemplate,
                this.JobFlowId,
                activityInfo.JobFlowDetail.Instances.MasterPublicDnsName ?? Resources.MasterPublicDnsNameNotDefined,
                activityInfo.CurrentState,
                EmrJobRunner.GetLatestRunningStepName(activityInfo.JobFlowDetail)));
        }

        private static String GetLatestRunningStepName(JobFlowDetail jobFlowDetail)
        {
            foreach (StepDetail stepDetail in jobFlowDetail.Steps)
            {
                if (stepDetail.ExecutionStatusDetail.State == StepExecutionState.RUNNING)
                    return stepDetail.StepConfig.Name;
            }

            return Resources.RunningStepNotDefined;
        }
    }
}
