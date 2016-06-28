package edu.unc.mapseq.commons.ncgenes.vcfcompare;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.module.sequencing.fastqc.FastQC;
import edu.unc.mapseq.module.sequencing.freebayes.FreeBayes;
import edu.unc.mapseq.module.sequencing.picard.PicardAddOrReplaceReadGroups;
import edu.unc.mapseq.module.sequencing.picard.PicardMarkDuplicates;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetrics;
import edu.unc.mapseq.workflow.SystemType;
import edu.unc.mapseq.workflow.sequencing.IRODSBean;

public class RegisterToIRODSRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RegisterToIRODSRunnable.class);

    private MaPSeqDAOBeanService mapseqDAOBeanService;

    private Long flowcellId;

    private Long sampleId;

    private SystemType system;

    private String workflowRunName;

    private Boolean includeMarkDuplicates;

    public RegisterToIRODSRunnable(MaPSeqDAOBeanService mapseqDAOBeanService, SystemType system, String workflowRunName,
            Boolean includeMarkDuplicates) {
        super();
        this.mapseqDAOBeanService = mapseqDAOBeanService;
        this.system = system;
        this.workflowRunName = workflowRunName;
        this.includeMarkDuplicates = includeMarkDuplicates;
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        Set<Sample> sampleSet = new HashSet<Sample>();
        SampleDAO sampleDAO = mapseqDAOBeanService.getSampleDAO();

        if (sampleId != null) {
            try {
                sampleSet.add(sampleDAO.findById(sampleId));
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        if (flowcellId != null) {
            try {
                List<Sample> samples = sampleDAO.findByFlowcellId(flowcellId);
                if (samples != null && !samples.isEmpty()) {
                    sampleSet.addAll(samples);
                }
            } catch (MaPSeqDAOException e1) {
                e1.printStackTrace();
                return;
            }
        }

        BundleContext bundleContext = FrameworkUtil.getBundle(getClass()).getBundleContext();
        Bundle bundle = bundleContext.getBundle();
        String version = bundle.getVersion().toString();

        for (Sample sample : sampleSet) {

            File outputDirectory = new File(sample.getOutputDirectory(), "NCGenesVCFCompare");
            File tmpDir = new File(outputDirectory, "tmp");
            if (!tmpDir.exists()) {
                tmpDir.mkdirs();
            }

            String participantId = null;
            for (Attribute attribute : sample.getAttributes()) {
                if ("subjectName".equals(attribute.getName())) {
                    participantId = attribute.getValue();
                    break;
                }
            }

            if (StringUtils.isEmpty(participantId)) {
                participantId = sample.getName();
            }

            String irodsDirectory = String.format("/MedGenZone/%s/sequencing/ncgenes/analysis/%s/%s/%s", system.getValue(),
                    sample.getFlowcell().getName(), sample.getName(), "NCGenesVCFCompare");

            CommandOutput commandOutput = null;

            List<CommandInput> commandInputList = new LinkedList<CommandInput>();

            CommandInput commandInput = new CommandInput();
            commandInput.setExitImmediately(Boolean.FALSE);
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("$IRODS_HOME/imkdir -p %s%n", irodsDirectory));
            commandInput.setCommand(sb.toString());
            commandInput.setWorkDir(tmpDir);
            commandInputList.add(commandInput);

            List<IRODSBean> files2RegisterToIRODS = new ArrayList<IRODSBean>();

            List<ImmutablePair<String, String>> attributeList = Arrays.asList(
                    new ImmutablePair<String, String>("ParticipantId", participantId),
                    new ImmutablePair<String, String>("MaPSeqWorkflowVersion", version),
                    new ImmutablePair<String, String>("MaPSeqWorkflowName", "NCGenesVCFCompare"),
                    new ImmutablePair<String, String>("MaPSeqStudyName", sample.getStudy().getName()),
                    new ImmutablePair<String, String>("MaPSeqSampleId", sample.getId().toString()),
                    new ImmutablePair<String, String>("MaPSeqSystem", system.getValue()),
                    new ImmutablePair<String, String>("MaPSeqFlowcellId", sample.getFlowcell().getId().toString()));

            List<ImmutablePair<String, String>> attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FastQC.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_ZIP.toString()));
            files2RegisterToIRODS.add(
                    new IRODSBean(new File(outputDirectory, String.format("%s.r1.fastqc.zip", workflowRunName)), attributeListWithJob));

            attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FastQC.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_ZIP.toString()));
            files2RegisterToIRODS.add(
                    new IRODSBean(new File(outputDirectory, String.format("%s.r2.fastqc.zip", workflowRunName)), attributeListWithJob));

            String file = String.format("%s.rg.bam", workflowRunName);

            attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob
                    .add(new ImmutablePair<String, String>("MaPSeqJobName", PicardAddOrReplaceReadGroups.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM.toString()));
            files2RegisterToIRODS.add(new IRODSBean(new File(outputDirectory, file), attributeListWithJob));

            if (includeMarkDuplicates) {
                file = String.format("%s.rg.deduped.bam", workflowRunName);
                attributeListWithJob = new ArrayList<>(attributeList);
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", PicardMarkDuplicates.class.getSimpleName()));
                attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.APPLICATION_BAM.toString()));
                files2RegisterToIRODS.add(new IRODSBean(new File(outputDirectory, file), attributeListWithJob));
            }

            attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", PicardCollectHsMetrics.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_PLAIN.toString()));
            files2RegisterToIRODS.add(new IRODSBean(new File(outputDirectory, file.replace(".bam", ".hs.metrics")), attributeListWithJob));

            attributeListWithJob = new ArrayList<>(attributeList);
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqJobName", FreeBayes.class.getSimpleName()));
            attributeListWithJob.add(new ImmutablePair<String, String>("MaPSeqMimeType", MimeType.TEXT_VCF.toString()));
            files2RegisterToIRODS.add(new IRODSBean(new File(outputDirectory, file.replace(".bam", ".vcf")), attributeListWithJob));

            for (IRODSBean bean : files2RegisterToIRODS) {

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);

                File f = bean.getFile();
                if (!f.exists()) {
                    logger.warn("file to register doesn't exist: {}", f.getAbsolutePath());
                    continue;
                }

                StringBuilder registerCommandSB = new StringBuilder();
                String registrationCommand = String.format("$IRODS_HOME/ireg -f %s %s/%s", bean.getFile().getAbsolutePath(), irodsDirectory,
                        bean.getFile().getName());
                String deRegistrationCommand = String.format("$IRODS_HOME/irm -U %s/%s", irodsDirectory, bean.getFile().getName());
                registerCommandSB.append(registrationCommand).append("\n");
                registerCommandSB.append(String.format("if [ $? != 0 ]; then %s; %s; fi%n", deRegistrationCommand, registrationCommand));
                commandInput.setCommand(registerCommandSB.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

                commandInput = new CommandInput();
                commandInput.setExitImmediately(Boolean.FALSE);
                sb = new StringBuilder();
                for (ImmutablePair<String, String> attribute : bean.getAttributes()) {
                    sb.append(String.format("$IRODS_HOME/imeta add -d %s/%s %s %s NCGenes%n", irodsDirectory, bean.getFile().getName(),
                            attribute.getLeft(), attribute.getRight()));
                }
                commandInput.setCommand(sb.toString());
                commandInput.setWorkDir(tmpDir);
                commandInputList.add(commandInput);

            }

            File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
            Executor executor = BashExecutor.getInstance();

            for (CommandInput ci : commandInputList) {
                try {
                    logger.debug("ci.getCommand(): {}", ci.getCommand());
                    commandOutput = executor.execute(ci, mapseqrc);
                    if (commandOutput.getExitCode() != 0) {
                        logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
                        logger.warn("command failed: {}", ci.getCommand());
                    }
                    logger.debug("commandOutput.getStdout(): {}", commandOutput.getStdout());
                } catch (ExecutorException e) {
                    if (commandOutput != null) {
                        logger.warn("commandOutput.getStderr(): {}", commandOutput.getStderr());
                    }
                }
            }

        }

    }

    public MaPSeqDAOBeanService getMapseqDAOBeanService() {
        return mapseqDAOBeanService;
    }

    public void setMapseqDAOBeanService(MaPSeqDAOBeanService mapseqDAOBeanService) {
        this.mapseqDAOBeanService = mapseqDAOBeanService;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public Long getFlowcellId() {
        return flowcellId;
    }

    public void setFlowcellId(Long flowcellId) {
        this.flowcellId = flowcellId;
    }

    public SystemType getSystem() {
        return system;
    }

    public void setSystem(SystemType system) {
        this.system = system;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

    public Boolean getIncludeMarkDuplicates() {
        return includeMarkDuplicates;
    }

    public void setIncludeMarkDuplicates(Boolean includeMarkDuplicates) {
        this.includeMarkDuplicates = includeMarkDuplicates;
    }

}
