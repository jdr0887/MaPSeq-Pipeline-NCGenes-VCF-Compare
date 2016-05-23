package edu.unc.mapseq.workflow.ncgenes.vcfcompare;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.Flowcell;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.sequencing.SureSelectTriggerSplitterCLI;
import edu.unc.mapseq.module.sequencing.bwa.BWAMEMCLI;
import edu.unc.mapseq.module.sequencing.fastqc.FastQCCLI;
import edu.unc.mapseq.module.sequencing.fastqc.IgnoreLevelType;
import edu.unc.mapseq.module.sequencing.freebayes.FreeBayesCLI;
import edu.unc.mapseq.module.sequencing.picard.PicardSortOrderType;
import edu.unc.mapseq.module.sequencing.picard2.PicardAddOrReplaceReadGroupsCLI;
import edu.unc.mapseq.module.sequencing.picard2.PicardCollectHsMetricsCLI;
import edu.unc.mapseq.module.sequencing.samtools.SAMToolsIndexCLI;
import edu.unc.mapseq.module.sequencing.vcflib.MergeVCFCLI;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.sequencing.AbstractSequencingWorkflow;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowJobFactory;
import edu.unc.mapseq.workflow.sequencing.SequencingWorkflowUtil;

public class NCGenesVCFCompareWorkflow extends AbstractSequencingWorkflow {

    private static final Logger logger = LoggerFactory.getLogger(NCGenesVCFCompareWorkflow.class);

    public NCGenesVCFCompareWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesVCFCompareWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/ncgenes/vcfcompare/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.1.0-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.info("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(CondorJobEdge.class);

        int count = 0;

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");
        String readGroupPlatform = getWorkflowBeanService().getAttributes().get("readGroupPlatform");
        String par1Coordinate = getWorkflowBeanService().getAttributes().get("par1Coordinate");
        String par2Coordinate = getWorkflowBeanService().getAttributes().get("par2Coordinate");
        String baitIntervalList = getWorkflowBeanService().getAttributes().get("baitIntervalList");
        String targetIntervalList = getWorkflowBeanService().getAttributes().get("targetIntervalList");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            logger.debug(sample.toString());

            Integer numberOfFreeBayesSubsets = 12;
            String gender = "M";
            Set<Attribute> workflowRunAttributeSet = workflowRun.getAttributes();
            if (CollectionUtils.isNotEmpty(workflowRunAttributeSet)) {
                for (Attribute attribute : workflowRunAttributeSet) {
                    if ("gender".equalsIgnoreCase(attribute.getName())) {
                        gender = attribute.getValue();
                        break;
                    }
                }
                for (Attribute attribute : workflowRunAttributeSet) {
                    if ("freeBayesJobCount".equalsIgnoreCase(attribute.getName())) {
                        numberOfFreeBayesSubsets = Integer.valueOf(attribute.getValue());
                        break;
                    }
                }
            }

            Flowcell flowcell = sample.getFlowcell();
            File workflowDirectory = new File(sample.getOutputDirectory(), getName());
            File tmpDirectory = new File(workflowDirectory, "tmp");
            tmpDirectory.mkdirs();

            int idx = sample.getName().lastIndexOf("-");
            String participantId = idx != -1 ? sample.getName().substring(0, idx) : sample.getName();

            List<File> readPairList = SequencingWorkflowUtil.getReadPairList(sample);
            logger.debug("readPairList.size(): {}", readPairList.size());

            if (readPairList.size() != 2) {
                throw new WorkflowException("readPairList != 2");
            }

            String rootFileName = workflowRun.getName();

            try {

                // new job
                CondorJobBuilder builder = SequencingWorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File r1FastqFile = readPairList.get(0);
                File fastqcR1Output = new File(workflowDirectory, String.format("%s.r1.fastqc.zip", rootFileName));
                builder.addArgument(FastQCCLI.INPUT, r1FastqFile.getAbsolutePath())
                        .addArgument(FastQCCLI.OUTPUT, fastqcR1Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());

                CondorJob fastQCR1Job = builder.build();
                logger.info(fastQCR1Job.toString());
                graph.addVertex(fastQCR1Job);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, FastQCCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File r2FastqFile = readPairList.get(1);
                File fastqcR2Output = new File(workflowDirectory, String.format("%s.r2.fastqc.zip", rootFileName));
                builder.addArgument(FastQCCLI.INPUT, r2FastqFile.getAbsolutePath())
                        .addArgument(FastQCCLI.OUTPUT, fastqcR2Output.getAbsolutePath())
                        .addArgument(FastQCCLI.IGNORE, IgnoreLevelType.ERROR.toString());
                CondorJob fastQCR2Job = builder.build();
                logger.info(fastQCR2Job.toString());
                graph.addVertex(fastQCR2Job);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, BWAMEMCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName).numberOfProcessors(4);
                File bwaMemOutFile = new File(workflowDirectory, String.format("%s.mem.sam", rootFileName));
                builder.addArgument(BWAMEMCLI.THREADS, "4").addArgument(BWAMEMCLI.VERBOSITY, "1")
                        .addArgument(BWAMEMCLI.FASTADB, referenceSequence).addArgument(BWAMEMCLI.FASTQ1, r1FastqFile.getAbsolutePath())
                        .addArgument(BWAMEMCLI.FASTQ2, r2FastqFile.getAbsolutePath())
                        .addArgument(BWAMEMCLI.OUTFILE, bwaMemOutFile.getAbsolutePath());
                CondorJob bwaMemJob = builder.build();
                logger.info(bwaMemJob.toString());
                graph.addVertex(bwaMemJob);
                graph.addEdge(fastQCR1Job, bwaMemJob);
                graph.addEdge(fastQCR2Job, bwaMemJob);

                // new job
                builder = SequencingWorkflowJobFactory
                        .createJob(++count, PicardAddOrReplaceReadGroupsCLI.class, attempt.getId(), sample.getId()).siteName(siteName);
                File fixRGOutput = new File(workflowDirectory, bwaMemOutFile.getName().replace(".sam", ".rg.bam"));
                String readGroupId = String.format("%s-%s.L%03d", flowcell.getName(), sample.getBarcode(), sample.getLaneIndex());
                builder.addArgument(PicardAddOrReplaceReadGroupsCLI.INPUT, bwaMemOutFile.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.OUTPUT, fixRGOutput.getAbsolutePath())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.SORTORDER, PicardSortOrderType.COORDINATE.toString().toLowerCase())
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPID, readGroupId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPLIBRARY, participantId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORM, readGroupPlatform)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPPLATFORMUNIT, readGroupId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPSAMPLENAME, participantId)
                        .addArgument(PicardAddOrReplaceReadGroupsCLI.READGROUPCENTERNAME, "UNC");
                CondorJob picardAddOrReplaceReadGroupsJob = builder.build();
                logger.info(picardAddOrReplaceReadGroupsJob.toString());
                graph.addVertex(picardAddOrReplaceReadGroupsJob);
                graph.addEdge(bwaMemJob, picardAddOrReplaceReadGroupsJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, SAMToolsIndexCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File picardAddOrReplaceReadGroupsIndexOut = new File(workflowDirectory, fixRGOutput.getName().replace(".bam", ".bai"));
                builder.addArgument(SAMToolsIndexCLI.INPUT, fixRGOutput.getAbsolutePath()).addArgument(SAMToolsIndexCLI.OUTPUT,
                        picardAddOrReplaceReadGroupsIndexOut.getAbsolutePath());
                CondorJob samtoolsIndexJob = builder.build();
                logger.info(samtoolsIndexJob.toString());
                graph.addVertex(samtoolsIndexJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, samtoolsIndexJob);

                // new job
                builder = SequencingWorkflowJobFactory.createJob(++count, PicardCollectHsMetricsCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName);
                File picardCollectHsMetricsFile = new File(workflowDirectory, fixRGOutput.getName().replace(".bam", ".hs.metrics"));
                builder.addArgument(PicardCollectHsMetricsCLI.INPUT, fixRGOutput.getAbsolutePath())
                        .addArgument(PicardCollectHsMetricsCLI.OUTPUT, picardCollectHsMetricsFile.getAbsolutePath())
                        .addArgument(PicardCollectHsMetricsCLI.REFERENCESEQUENCE, referenceSequence)
                        .addArgument(PicardCollectHsMetricsCLI.BAITINTERVALS, baitIntervalList)
                        .addArgument(PicardCollectHsMetricsCLI.TARGETINTERVALS, targetIntervalList);

                CondorJob picardCollectHsMetricsJob = builder.build();
                logger.info(picardCollectHsMetricsJob.toString());
                graph.addVertex(picardCollectHsMetricsJob);
                graph.addEdge(samtoolsIndexJob, picardCollectHsMetricsJob);

                // new job
                builder = SequencingWorkflowJobFactory
                        .createJob(++count, SureSelectTriggerSplitterCLI.class, attempt.getId(), sample.getId()).siteName(siteName)
                        .initialDirectory(workflowDirectory.getAbsolutePath());
                File ploidyFile = new File(workflowDirectory, String.format("%s.ploidy.bed", participantId));
                builder.addArgument(SureSelectTriggerSplitterCLI.GENDER, gender)
                        .addArgument(SureSelectTriggerSplitterCLI.INTERVALLIST, targetIntervalList)
                        .addArgument(SureSelectTriggerSplitterCLI.SUBJECTNAME, participantId)
                        .addArgument(SureSelectTriggerSplitterCLI.NUMBEROFSUBSETS, numberOfFreeBayesSubsets)
                        .addArgument(SureSelectTriggerSplitterCLI.PAR1COORDINATE, par1Coordinate)
                        .addArgument(SureSelectTriggerSplitterCLI.PAR2COORDINATE, par2Coordinate)
                        .addArgument(SureSelectTriggerSplitterCLI.OUTPUTPREFIX, String.format("%s_Trg", participantId));
                CondorJob sureSelectTriggerSplitterJob = builder.build();
                logger.info(sureSelectTriggerSplitterJob.toString());
                graph.addVertex(sureSelectTriggerSplitterJob);
                graph.addEdge(picardAddOrReplaceReadGroupsJob, sureSelectTriggerSplitterJob);

                List<CondorJob> mergeVCFParentJobs = new ArrayList<CondorJob>();

                for (int i = 0; i < numberOfFreeBayesSubsets; i++) {

                    // new job
                    builder = SequencingWorkflowJobFactory.createJob(++count, FreeBayesCLI.class, attempt.getId(), sample.getId())
                            .siteName(siteName);
                    File freeBayesOutput = new File(workflowDirectory, String.format("%s_Trg.set%d.vcf", participantId, i + 1));
                    File targetFile = new File(workflowDirectory, String.format("%s_Trg.interval.set%d.bed", participantId, i + 1));
                    builder.addArgument(FreeBayesCLI.GENOTYPEQUALITIES).addArgument(FreeBayesCLI.REPORTMONOMORPHIC)
                            .addArgument(FreeBayesCLI.BAM, fixRGOutput.getAbsolutePath())
                            .addArgument(FreeBayesCLI.VCF, freeBayesOutput.getAbsolutePath())
                            .addArgument(FreeBayesCLI.FASTAREFERENCE, referenceSequence)
                            .addArgument(FreeBayesCLI.TARGETS, targetFile.getAbsolutePath())
                            .addArgument(FreeBayesCLI.COPYNUMBERMAP, ploidyFile.getAbsolutePath());
                    CondorJob freeBayesJob = builder.build();
                    logger.info(freeBayesJob.toString());
                    graph.addVertex(freeBayesJob);
                    graph.addEdge(sureSelectTriggerSplitterJob, freeBayesJob);
                    mergeVCFParentJobs.add(freeBayesJob);
                }

                builder = SequencingWorkflowJobFactory.createJob(++count, MergeVCFCLI.class, attempt.getId(), sample.getId())
                        .siteName(siteName).initialDirectory(workflowDirectory.getAbsolutePath());
                File mergeVCFOutput = new File(workflowDirectory, String.format("%s.vcf", participantId));
                builder.addArgument(MergeVCFCLI.INPUT, String.format("%s_Trg.*.vcf", participantId))
                        .addArgument(MergeVCFCLI.WORKDIRECTORY, workflowDirectory.getAbsolutePath())
                        .addArgument(MergeVCFCLI.OUTPUT, mergeVCFOutput.getAbsolutePath());
                CondorJob mergeVCFJob = builder.build();
                logger.info(mergeVCFJob.toString());
                graph.addVertex(mergeVCFJob);
                for (CondorJob job : mergeVCFParentJobs) {
                    graph.addEdge(job, mergeVCFJob);
                }

            } catch (Exception e) {
                throw new WorkflowException(e);
            }
        }

        return graph;
    }

    // @Override
    // public void postRun() throws WorkflowException {
    // logger.info("ENTERING postRun()");
    //
    // Set<Sample> sampleSet = getAggregatedSamples();
    //
    // ExecutorService executorService = Executors.newSingleThreadExecutor();
    //
    // for (Sample sample : sampleSet) {
    //
    // if ("Undetermined".equals(sample.getBarcode())) {
    // continue;
    // }
    //
    // MaPSeqDAOBeanService daoBean = getWorkflowBeanService().getMaPSeqDAOBeanService();
    // MaPSeqConfigurationService configService = getWorkflowBeanService().getMaPSeqConfigurationService();
    //
    // RegisterToIRODSRunnable registerToIRODSRunnable = new RegisterToIRODSRunnable();
    // registerToIRODSRunnable.setMapseqDAOBean(daoBean);
    // registerToIRODSRunnable.setMapseqConfigurationService(configService);
    // registerToIRODSRunnable.setSampleId(sample.getId());
    // executorService.submit(registerToIRODSRunnable);
    //
    // SaveCollectHsMetricsAttributesRunnable saveCollectHsMetricsAttributesRunnable = new
    // SaveCollectHsMetricsAttributesRunnable();
    // saveCollectHsMetricsAttributesRunnable.setMapseqDAOBeanService(daoBean);
    // saveCollectHsMetricsAttributesRunnable.setSampleId(sample.getId());
    // executorService.submit(saveCollectHsMetricsAttributesRunnable);
    //
    // }
    //
    // executorService.shutdown();
    //
    // }

}
