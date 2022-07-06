package com.rui.dp.prj;

import com.rui.dp.prj.base.ExternalResourceSyncJobDataLoader;
import com.rui.dp.prj.base.PackageResourceSyncJobDataLoader;
import com.rui.dp.prj.base.job.DeepStreamSyncJobData;

public class SyncProjectStarter {
    public void start(String[] args) {
        DeepStreamSyncJobData jobData;
        if (args == null || args.length == 0) {
            jobData = PackageResourceSyncJobDataLoader.load();
        } else {
            ExternalResourceSyncJobDataLoader loader = new ExternalResourceSyncJobDataLoader(args[0]);
            jobData = loader.load();
        }

        DataSyncJob job = new DataSyncJob(jobData);
        job.prepare();
        job.start();
        job.clean();
    }
}
