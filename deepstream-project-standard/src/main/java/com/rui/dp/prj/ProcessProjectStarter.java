package com.rui.dp.prj;

import com.rui.dp.prj.base.job.DeepStreamProcessJobData;
import com.rui.dp.prj.base.ExternalResourceJobDataLoader;
import com.rui.dp.prj.base.PackageResourceJobDataLoader;

abstract class ProcessProjectStarter {

    public void start(String[] args) {
        DeepStreamProcessJobData jobData;
        if (args == null || args.length == 0) {
            jobData = PackageResourceJobDataLoader.load();
        } else {
            ExternalResourceJobDataLoader loader = new ExternalResourceJobDataLoader(args[0]);
            jobData = loader.load();
        }

        DataProcessJob job = new DataProcessJob(jobData);
        job.prepare();
        job.start();
        job.clean();
    }

}
