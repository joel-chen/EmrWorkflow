using Amazon.ElasticMapReduce;
using EmrWorkflow.Run.Model;
using System;
using System.Threading.Tasks;

namespace EmrWorkflow.Run
{
    /// <summary>
    /// An interface to check the current state of the EMR Job
    /// </summary>
    public interface IEmrJobStateChecker
    {
        /// <summary>
        /// Send a request to the EMR service to get the latest state of the job
        /// </summary>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="jobFlowId">EMR Job flow id</param>
        /// <returns>Current state of the EMR Job</returns>
        Task<EmrActivityInfo> CheckAsync(IAmazonElasticMapReduce emrClient, String jobFlowId);
    }
}
