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
    public abstract class EmrJobRunner
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
        /// Current activity index in the list <see cref="EmrActivities"/>
        /// </summary>
        private int currentActivityIndex;

        public EmrJobRunner()
        {
            this.currentActivityIndex = 0;
            this.isBusy = 0;
            this.threadTimer = new Timer(this.CheckStatus);
        }

        public bool IsRunning
        {
            get { return this.threadTimer != null; }
        }

        public string JobFlowId
        {
            get { return this.Settings.Get(BuilderSettings.JobFlowId); }
            set
            {
                this.Settings.Put(BuilderSettings.JobFlowId, value);
            }
        }

        public abstract BuilderSettings Settings { get; }

        public abstract AmazonElasticMapReduceClient EmrClient { get; }

        public abstract IList<EmrActivityStrategy> EmrActivities { get; }

        public async void Run()
        {
            if ((await this.PushNextActivity()))
                this.threadTimer.Change(0, timerPeriod);
            else
                this.StopRunning();
        }

        private async Task<bool> PushNextActivity()
        {
            if (this.currentActivityIndex == this.EmrActivities.Count)
            {
                Console.WriteLine("Done...");
                return false;
            }

            //TODO: add retry cycle
            EmrActivityStrategy activity = this.EmrActivities[this.currentActivityIndex];
            Console.WriteLine("Adding activity: " + activity.Name);

            bool pushResult;
            try
            {
                pushResult = await activity.PushAsync(this);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception during sending the request: " + ex.Message);
                return false;
            }

            if (!pushResult)
            {
                Console.WriteLine("Request failed...");
                return false;
            }

            this.currentActivityIndex++;
            return true;
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
            else if (activityInfo.CurrentState == EmrActivityState.Completed)
            {
                if (!(await this.PushNextActivity()))
                    this.StopRunning();
            }
            else if (activityInfo.CurrentState == EmrActivityState.Failed)
            {
                String errorMessage = activityInfo.JobFlowDetail.ExecutionStatusDetail.LastStateChangeReason;
                Console.WriteLine(String.Format(Resources.FailToRunJobTemplate, errorMessage));
                this.StopRunning();
            }

            Interlocked.Exchange(ref this.isBusy, 0);
        }

        private void StopRunning()
        {
            this.threadTimer.Dispose();
            this.threadTimer = null;
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
