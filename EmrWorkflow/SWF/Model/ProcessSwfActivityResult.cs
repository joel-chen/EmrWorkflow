using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EmrWorkflow.SWF.Model
{
    class ProcessSwfActivityResult
    {
        public SwfActivity Output { get; set; }

        public string ErrorMessage { get; set; }
    }
}
