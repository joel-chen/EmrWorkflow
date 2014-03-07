using Amazon.ElasticMapReduce;
using Amazon.SimpleWorkflow;
using Amazon.SimpleWorkflow.Model;
using EmrWorkflow.RequestBuilders;
using EmrWorkflow.Run;
using System;
using System.Threading.Tasks;

namespace EmrPlusSwf
{
    public class SwfEmrJobActivityProcessor : EmrJobManagerBase
    {
        /// <summary>
        /// Constructor for injecting dependencies
        /// </summary>
        /// <param name="emrJobLogger">Instantiated object to log information about the EMR Job</param>
        /// <param name="swfClient">Instantiated SWF Client to make requests to the Amazon SWF Service</param>
        /// <param name="emrClient">Instantiated EMR Client to make requests to the Amazon EMR Service</param>
        /// <param name="emrJobStateChecker">Instantiated object to check the current state of the EMR Job</param>
        /// <param name="settings">Settings to replace placeholders</param>
        /// <param name="emrActivitiesEnumerator">Iterator through the job flow's activities</param>
        public SwfEmrJobActivityProcessor(IEmrJobLogger emrJobLogger, IAmazonSimpleWorkflow swfClient, IAmazonElasticMapReduce emrClient, IEmrJobStateChecker emrJobStateChecker, BuilderSettings settings, EmrActivitiesEnumerator emrActivitiesEnumerator)
            : base(emrJobLogger, emrClient, emrJobStateChecker, settings)
        {
            this.SwfClient = swfClient;
        }

        /// <summary>
        /// SWF Client to make requests to the Amazon SWF Service
        /// </summary>
        public IAmazonSimpleWorkflow SwfClient { get; set; }

        protected async override void DoWorkSafe()
        {
            ActivityTask activityTask = await this.Poll();
            if (!String.IsNullOrEmpty(activityTask.TaskToken))
            {
                ActivityState activityState = ProcessTask(activityTask.Input);
                CompleteTask(activityTask.TaskToken, activityState);
            }
        }

        private async Task<ActivityTask> Poll()
        {
            EmrJobLogger.PrintInfo("Polling for activity task ...");
            PollForActivityTaskRequest request = new PollForActivityTaskRequest()
            {
                Domain = Constants.EmrJobDomain,
                TaskList = new TaskList()
                {
                    Name = Constants.ImageProcessingActivityTaskList
                }
            };
            PollForActivityTaskResponse response = await this.SwfClient.PollForActivityTaskAsync(request);
            return response.ActivityTask;
        }

        private ActivityState ProcessTask(string input)
        {
            ActivityState activityState = Utils.DeserializeFromJSON<ActivityState>(input);
            this._console.WriteLine(string.Format("Processing activity task (Resize Image {0}x{0})...", activityState.ImageSize));

            var getRequest = new GetObjectRequest
            {
                BucketName = activityState.StartingInput.Bucket,
                Key = activityState.StartingInput.SourceImageKey
            };

            // Get the image from S3. Response is wrapped in a using statement so the
            // stream coming back from S3 is closed.
            //
            // To keep the sample simple the source image is downloaded for each thumbnail.
            // This could be cached locally for better performance.
            using (var getResponse = this.s3Client.GetObject(getRequest))
            {
                // Resize the image
                Stream thumbnailStream = ResizeImage(getResponse.ResponseStream, activityState.ImageSize);

                activityState.ResizedImageKey = String.Format("thumbnails/{0}x{0}/{1}", activityState.ImageSize, activityState.StartingInput.SourceImageKey);
                var putRequest = new PutObjectRequest
                {
                    BucketName = activityState.StartingInput.Bucket,
                    Key = activityState.ResizedImageKey,
                    InputStream = thumbnailStream
                };
                s3Client.PutObject(putRequest);
            }
            return activityState;
        }
    }
}
