using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Run.Model;
using EmrWorkflow.Run.Strategies;
using System;

namespace EmrWorkflow.Run
{
    static class EmrJobLogger
    {
        public static void PrintCompleted(bool hasErrors)
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

        public static void PrintCheckingStatus()
        {
            Console.ForegroundColor = ConsoleColor.Gray;
            Console.WriteLine(Resources.Info_CheckingJobStatus);
            Console.ResetColor();
        }

        public static void PrintAddingNewActivity(EmrActivityStrategy activity)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(String.Format(Resources.Info_AddingActivityTemplate, activity.Name));
            Console.ResetColor();
        }

        public static void PrintError(EmrActivityInfo activityInfo)
        {
            String errorMessage = activityInfo.JobFlowDetail.ExecutionStatusDetail.LastStateChangeReason;
            EmrJobLogger.PrintError(String.Format(Resources.Info_FailToRunJobTemplate, errorMessage));
        }

        public static void PrintError(string errorMessage)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ResetColor();
        }

        public static void PrintJobInfo(EmrActivityInfo activityInfo)
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
