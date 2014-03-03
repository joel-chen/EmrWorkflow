using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    public class EmrJobRunner : IDisposable
    {
        private const int timerPeriod = 60000 * 1; //every 1 minute

        /// <summary>
        /// Internal field used for sync access to the <see cref="CheckStatus"/> method called by the timer
        /// </summary>
        private int isBusy;

        /// <summary>
        /// A value which indicates the disposable state. 0 indicates undisposed, 1 indicates disposing
        /// or disposed.
        /// </summary>
        private int disposableState;

        /// <summary>
        /// Timer for calling <see cref="CheckStatus"/> method
        /// </summary>
        private Timer threadTimer;

        /// <summary>
        /// Internal field to indicate if job had errors
        /// </summary>
        private bool hasErrors;

        /// <summary>
        /// A reference to the activities list from the <see cref="EmrActivitiesEnumerator"/>
        /// </summary>
        private IEnumerator<EmrActivityStrategy> activities;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public EmrJobRunner(BuilderSettings settings, AmazonElasticMapReduceClient emrClient, EmrActivitiesEnumerator emrActivitiesEnumerator)
        {
            this.isBusy = 0;
            this.hasErrors = false;
            this.threadTimer = new Timer(this.CheckStatus);

            this.Settings = settings;
            this.EmrClient = emrClient;
            this.EmrActivitiesEnumerator = emrActivitiesEnumerator;
        }

        /// <summary>
        /// If the job is running
        /// </summary>
        public bool IsRunning
        {
            get { return Thread.VolatileRead(ref this.disposableState) == 0; }
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
        public EmrActivitiesEnumerator EmrActivitiesEnumerator { get; set; }

        /// <summary>
        /// Start the job flow
        /// </summary>
        public async void Run()
        {
            this.activities = this.EmrActivitiesEnumerator.GetActivities(this).GetEnumerator();

            if ((await this.PushNextActivity()))
                this.threadTimer.Change(0, timerPeriod);
            else
                this.Dispose();
        }

        private async void CheckStatus(Object stateInfo)
        {
            if (Interlocked.CompareExchange(ref this.isBusy, 1, 0) != 0)
                return;

            EmrJobLogger.PrintCheckingStatus();
            EmrJobStateChecker jobStateChecker = new EmrJobStateChecker();
            EmrActivityInfo activityInfo = jobStateChecker.Check(this.EmrClient, this.JobFlowId);

            if (activityInfo.CurrentState == EmrActivityState.Running)
            {
                EmrJobLogger.PrintJobInfo(activityInfo);
            }
            else
            {
                if (activityInfo.CurrentState == EmrActivityState.Failed)
                {
                    this.hasErrors = true;
                    EmrJobLogger.PrintError(activityInfo);
                    this.EmrActivitiesEnumerator.NotifyJobFailed(this);
                }

                if (!(await this.PushNextActivity()))
                    this.Dispose();
            }

            Interlocked.Exchange(ref this.isBusy, 0);
        }

        private async Task<bool> PushNextActivity()
        {
            if (!this.activities.MoveNext())
            {
                EmrJobLogger.PrintCompleted(this.hasErrors);
                return false;
            }

            EmrActivityStrategy activity = this.activities.Current;
            EmrJobLogger.PrintAddingNewActivity(activity);

            //TODO: probably add a retry cycle
            bool pushResult;
            try
            {
                pushResult = await activity.PushAsync(this);
            }
            catch (Exception ex)
            {
                EmrJobLogger.PrintError(String.Format(Resources.Info_ExceptionWhenSendingRequestTemplate, ex.Message));
                return false;
            }

            if (!pushResult)
            {
                EmrJobLogger.PrintError(Resources.Info_EmrServiceNotOkResponse);
                return false;
            }

            return true;
        }

        public void Dispose()
        {
            // Attempt to move the disposable state from 0 to 1. If successful, we can be assured that
            // this thread is the first thread to do so, and can safely dispose of the object.
            if (Interlocked.CompareExchange(ref this.disposableState, 1, 0) != 0)
                return;

            if (this.activities != null)
            {
                this.activities.Dispose();
                this.activities = null;
            }

            if (this.threadTimer != null)
            {
                this.threadTimer.Dispose();
                this.threadTimer = null;
            }

            GC.SuppressFinalize(this);
        }
    }
}
