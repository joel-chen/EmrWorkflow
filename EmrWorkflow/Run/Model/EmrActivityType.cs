namespace EmrWorkflow.Run.Model
{
    public enum EmrActivityType
    {
        /// <summary>
        /// Activity "Start a new cluster to run an EMR Job"
        /// </summary>
        StartJob,

        /// <summary>
        /// Activity "Add steps to an existing EMR Job"
        /// </summary>
        AddSteps,

        /// <summary>
        /// Activity "Terminate an existing EMR Job"
        /// </summary>
        TerminateJob
    }
}
