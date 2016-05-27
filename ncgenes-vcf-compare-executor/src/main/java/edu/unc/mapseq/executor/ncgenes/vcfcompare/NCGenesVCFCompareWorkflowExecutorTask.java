package edu.unc.mapseq.executor.ncgenes.vcfcompare;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.ncgenes.vcfcompare.NCGenesVCFCompareWorkflow;

public class NCGenesVCFCompareWorkflowExecutorTask extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesVCFCompareWorkflowExecutorTask.class);

    private static final WorkflowTPE threadPoolExecutor = new WorkflowTPE();

    private WorkflowBeanService workflowBeanService;

    private String workflowName;

    public NCGenesVCFCompareWorkflowExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(workflowBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(workflowBeanService.getMaxPoolSize());

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d", threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getTaskCount(), threadPoolExecutor.getCompletedTaskCount()));

        MaPSeqDAOBeanService mapseqDAOBeanService = this.workflowBeanService.getMaPSeqDAOBeanService();
        WorkflowDAO workflowDAO = mapseqDAOBeanService.getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = mapseqDAOBeanService.getWorkflowRunAttemptDAO();

        try {

            Workflow workflow = null;
            List<Workflow> workflowList = workflowDAO.findByName(getWorkflowName());
            if (CollectionUtils.isEmpty(workflowList)) {
                workflow = new Workflow(getWorkflowName());
                workflow.setId(workflowDAO.save(workflow));
            } else {
                workflow = workflowList.get(0);
            }

            if (workflow == null) {
                logger.error("Could not find or create {} workflow", getWorkflowName());
                return;
            }

            List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findEnqueued(workflow.getId());
            if (CollectionUtils.isNotEmpty(attempts)) {
                logger.info("dequeuing {} WorkflowRunAttempt", attempts.size());
                for (WorkflowRunAttempt attempt : attempts) {

                    NCGenesVCFCompareWorkflow alignmentWorkflow = new NCGenesVCFCompareWorkflow();
                    attempt.setVersion(alignmentWorkflow.getVersion());
                    attempt.setDequeued(new Date());
                    workflowRunAttemptDAO.save(attempt);

                    alignmentWorkflow.setWorkflowBeanService(workflowBeanService);
                    alignmentWorkflow.setWorkflowRunAttempt(attempt);
                    threadPoolExecutor.submit(new WorkflowExecutor(alignmentWorkflow));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public String getWorkflowName() {
        return workflowName;
    }

    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

}
