﻿using Amazon.ElasticMapReduce;
using Amazon.ElasticMapReduce.Model;
using EmrWorkflow.Run.Model;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace EmrWorkflow.Run.Implementation
{
    //TODO: cover with test
    /// <summary>
    /// A class to check the current state of the EMR Job
    /// </summary>
    public class EmrJobStateChecker : IEmrJobStateChecker
    {
        /// <summary>
        /// Send a request to the EMR service to get the latest state of the job
        /// </summary>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="jobFlowId">EMR Job flow id</param>
        /// <returns>Current state of the EMR Job</returns>
        public async Task<EmrActivityInfo> CheckAsync(IAmazonElasticMapReduce emrClient, String jobFlowId)
        {
            //Read job state
            DescribeJobFlowsRequest request = new DescribeJobFlowsRequest();
            request.JobFlowIds = new List<string>() { jobFlowId };
            DescribeJobFlowsResponse response = await emrClient.DescribeJobFlowsAsync(request);
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
