using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Run.Model;
using System;

namespace EmrWorkflow.Run.Implementation
{
    /// <summary>
    /// A class to log information about the EMR Job
    /// </summary>
    public class EmrJobLogger : IEmrJobLogger
    {
        /// <summary>
        /// Print a specified message
        /// </summary>
        /// <param name="infoMessage">Message</param>
        public void PrintInfo(string infoMessage)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(infoMessage);
            Console.ResetColor();
        }

        /// <summary>
        /// Print that a job completed
        /// </summary>
        /// <param name="hasErrors">A flag that indicates that a job completed with errors</param>
        public void PrintCompleted(bool hasErrors)
        {
            Console.WriteLine();
            if (hasErrors)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(Resources.Info_CompletedWithErrors);
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(Resources.Info_CompletedSuccessfully);
            }
            Console.ResetColor();
        }

        /// <summary>
        /// Print that a process is checking the status of the EMR Job
        /// </summary>
        public void PrintCheckingStatus()
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine(Resources.Info_CheckingJobStatus);
            Console.ResetColor();
        }

        /// <summary>
        /// Print that a process is adding new activity to the EMR Job
        /// </summary>
        /// <param name="activityName">An activity's name to be added to the EMR Job</param>
        public void PrintAddingNewActivity(string activityName)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(String.Format(Resources.Info_AddingActivityTemplate, activityName));
            Console.ResetColor();
        }

        /// <summary>
        /// Print that EMR Job has an error
        /// </summary>
        /// <param name="activityInfo">Current state of the job and current activity</param>
        public void PrintError(EmrActivityInfo activityInfo)
        {
            String errorMessage = activityInfo.JobFlowDetail.ExecutionStatusDetail.LastStateChangeReason;
            this.PrintError(String.Format(Resources.Info_FailToRunJobTemplate, errorMessage));
        }

        /// <summary>
        /// Print error message
        /// </summary>
        /// <param name="errorMessage">Error message</param>
        public void PrintError(string errorMessage)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ResetColor();
        }

        /// <summary>
        /// Print current EMR Job's state
        /// </summary>
        /// <param name="activityInfo">Current state of the job and current activity</param>
        public void PrintJobInfo(EmrActivityInfo activityInfo)
        {
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(Resources.Info_JobFlowId);
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(activityInfo.JobFlowDetail.JobFlowId);
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine(String.Format(Resources.Info_JobCurrentStateTemplate,
                EmrJobLogger.GetLatestRunningStepName(activityInfo.JobFlowDetail),
                activityInfo.JobFlowDetail.ExecutionStatusDetail.State,
                (activityInfo.JobFlowDetail.Instances.MasterPublicDnsName ?? Resources.Info_MasterPublicDnsNameNotDefined)));
            Console.ResetColor();
        }

        private static String GetLatestRunningStepName(JobFlowDetail jobFlowDetail)
        {
            foreach (StepDetail stepDetail in jobFlowDetail.Steps)
            {
                if (stepDetail.ExecutionStatusDetail.State == StepExecutionState.RUNNING)
                    return stepDetail.StepConfig.Name;
            }

            return Resources.Info_RunningStepNotDefined;
        }
    }
}
