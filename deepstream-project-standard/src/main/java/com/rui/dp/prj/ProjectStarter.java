package com.rui.dp.prj;

import com.rui.dp.prj.base.DeepStreamProcessJobData;
import com.rui.dp.prj.base.PackageResourceJobDataLoader;

public class ProjectStarter {
    public static void main(String[] args) {
        DeepStreamProcessJobData jobData = PackageResourceJobDataLoader.load();

        DataProcessJob job = new DataProcessJob(jobData);
        job.prepare();
        job.start();
        job.clean();
    }
}
