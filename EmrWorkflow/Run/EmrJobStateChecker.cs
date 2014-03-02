using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Run.Model;
using System;
using System.Collections.Generic;
using System.Net;

namespace EmrWorkflow.Run
{
    //TODO: cover with test
    public class EmrJobStateChecker
    {
        public EmrActivityInfo Check(AmazonElasticMapReduceClient emrClient, String jobFlowId)
        {
            //Read job state
            DescribeJobFlowsRequest request = new DescribeJobFlowsRequest();
            request.JobFlowIds = new List<string>() { jobFlowId };
            DescribeJobFlowsResponse response = emrClient.DescribeJobFlows(request);
            if (response.HttpStatusCode != HttpStatusCode.OK)
                return new EmrActivityInfo() { CurrentState = EmrActivityState.Failed };

            //Map job state into Completed, Failed or Running
            JobFlowDetail jobFlowDetail = response.JobFlows[0];
            EmrActivityState activityState = EmrJobStateChecker.GetState(jobFlowDetail);
            return new EmrActivityInfo() { JobFlowDetail = jobFlowDetail, CurrentState = activityState };
        }

        private static EmrActivityState GetState(JobFlowDetail jobFlowDetail)
        {
            JobFlowExecutionState latestState = jobFlowDetail.ExecutionStatusDetail.State;

            if (latestState == JobFlowExecutionState.COMPLETED
                || latestState == JobFlowExecutionState.WAITING
                || latestState == JobFlowExecutionState.TERMINATED)
            {
                return EmrJobStateChecker.GetStateFromLastStep(jobFlowDetail);
            }
            else if (latestState == JobFlowExecutionState.FAILED)
            {
                return EmrActivityState.Failed;
            }
            else
            {
                return EmrActivityState.Running;
            }
        }

        private static EmrActivityState GetStateFromLastStep(JobFlowDetail jobFlowDetail)
        {
            List<StepDetail> steps = jobFlowDetail.Steps;

            if (steps.Count == 0)
                return EmrActivityState.Completed;

            StepDetail lastStep = steps[steps.Count - 1];
            StepExecutionState lastStepState = lastStep.ExecutionStatusDetail.State;

            if (lastStepState == StepExecutionState.PENDING)
            {
                return EmrActivityState.Running;
            }
            else if (lastStepState != StepExecutionState.COMPLETED)
            {
                return EmrActivityState.Failed;
            }
            else
            {
                return EmrActivityState.Completed;
            }
        }
    }
}
