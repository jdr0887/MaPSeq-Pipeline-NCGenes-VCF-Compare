package edu.unc.mapseq.commands.ncgenes.vcfcompare;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.commons.ncgenes.vcfcompare.RegisterToIRODSRunnable;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.workflow.SystemType;

@Command(scope = "ncgenes-vcfcompare", name = "register-to-irods", description = "Register a sample output to iRODS")
@Service
public class RegisterToIRODSAction implements Action {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSAction.class);

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Option(name = "--sampleId", description = "Sample Identifier", required = false, multiValued = false)
    private Long sampleId;

    @Option(name = "--flowcellId", description = "Flowcell Identifier", required = false, multiValued = false)
    private Long flowcellId;

    @Option(name = "--workflowRunId", description = "WorkflowRun Identifier", required = true, multiValued = false)
    private Long workflowRunId;

    @Option(name = "--includeMarkDuplicates", description = "includeMarkDuplicates", required = false, multiValued = false)
    private Boolean includeMarkDuplicates = Boolean.TRUE;

    @Override
    public Object execute() {
        logger.debug("ENTERING execute()");
        try {
            WorkflowRun workflowRun = maPSeqDAOBeanService.getWorkflowRunDAO().findById(workflowRunId);

            ExecutorService es = Executors.newSingleThreadExecutor();
            RegisterToIRODSRunnable runnable = new RegisterToIRODSRunnable(maPSeqDAOBeanService, SystemType.EXPERIMENTAL,
                    workflowRun.getName(), includeMarkDuplicates);
            if (sampleId != null) {
                runnable.setSampleId(sampleId);
            }
            if (flowcellId != null) {
                runnable.setFlowcellId(flowcellId);
            }
            es.submit(runnable);
            es.shutdown();
        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

}
