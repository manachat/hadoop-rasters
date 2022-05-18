package vafilonov.hadooprasters.frontend.model;

import org.apache.hadoop.mapreduce.Job;

abstract class ProcessingStage {

    protected final JobProcessingContext context;

    protected Job hadoopJob;

    Job getAssociatedJob() {
        return hadoopJob;
    }

    ProcessingStage(JobProcessingContext context) {
        this.context = context;
    }

}
