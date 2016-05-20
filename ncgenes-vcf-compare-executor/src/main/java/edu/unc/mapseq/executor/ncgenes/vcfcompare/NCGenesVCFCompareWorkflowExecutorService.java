package edu.unc.mapseq.executor.ncgenes.vcfcompare;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesVCFCompareWorkflowExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesVCFCompareWorkflowExecutorService.class);

    private static final Timer mainTimer = new Timer();

    private NCGenesVCFCompareWorkflowExecutorTask task;

    private Long period = 5L;

    public NCGenesVCFCompareWorkflowExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000;
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesVCFCompareWorkflowExecutorTask getTask() {
        return task;
    }

    public void setTask(NCGenesVCFCompareWorkflowExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
