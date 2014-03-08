using EmrWorkflow.Model.BootstrapActions;
using EmrWorkflow.Model.Configs;
using EmrWorkflow.Model.Steps;
using EmrWorkflow.Model.Tags;

namespace EmrWorkflow.Model
{
    public interface IEmrWorkflowItemVisitor
    {
        void Visit(JobFlow jobFlow);

        void Visit(ClusterTag clusterTag);

        void Visit(DebugConfig debugConfig);

        void Visit(HBaseConfig hBaseConfig);

        void Visit(HBaseDaemonsConfig hBaseDaemonsConfig);

        void Visit(HadoopConfig hadoopConfig);

        void Visit(BootstrapAction bootstrapAction);

        void Visit(JarStep step);

        void Visit(HBaseRestoreStep hBaseRestoreStep);

        void Visit(HBaseBackupStep hBaseBackupStep);
    }
}
