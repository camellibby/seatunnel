/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.seatunnel.core.starter.flink.utils.HttpUtil;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigUtil;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.execution.RuntimeEnvironment;
import org.apache.seatunnel.core.starter.execution.TaskExecution;
import org.apache.seatunnel.core.starter.flink.FlinkStarter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Used to execute a SeaTunnelTask.
 */
//@Slf4j
public class FlinkExecution implements TaskExecution {

    private static final Logger log = LoggerFactory.getLogger(FlinkExecution.class);
    private final FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    private final PluginExecuteProcessor<DataStream<Row>, FlinkRuntimeEnvironment>
            sourcePluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStream<Row>, FlinkRuntimeEnvironment>
            transformPluginExecuteProcessor;
    private final PluginExecuteProcessor<DataStream<Row>, FlinkRuntimeEnvironment>
            sinkPluginExecuteProcessor;
    private final List<URL> jarPaths;

    private JobContext jobContext;


    public FlinkExecution(Config config) {
        this.jobContext = new JobContext();
        try {
            jarPaths =
                    new ArrayList<>(
                            Collections.singletonList(
                                    new File(
                                            Common.appStarterDir()
                                                    .resolve(FlinkStarter.APP_JAR_NAME)
                                                    .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }
        registerPlugin(config.getConfig("env"));
        jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));
        try {
            int isRecordErrorData = config.getConfig("env").getInt("isRecordErrorData");
            int maxRecordNumber = config.getConfig("env").getInt("maxRecordNumber");
            jobContext.setIsRecordErrorData(isRecordErrorData);
            jobContext.setMaxRecordNumber(maxRecordNumber);
        } catch (Exception e) {
            log.info("");
        }
        this.sourcePluginExecuteProcessor =
                new SourceExecuteProcessor(
                        jarPaths, config.getConfigList(Constants.SOURCE), jobContext);
        this.transformPluginExecuteProcessor =
                new TransformExecuteProcessor(
                        jarPaths,
                        TypesafeConfigUtils.getConfigList(
                                config, Constants.TRANSFORM, Collections.emptyList()),
                        jobContext);
        this.sinkPluginExecuteProcessor =
                new SinkExecuteProcessor(
                        jarPaths, config.getConfigList(Constants.SINK), jobContext);

        this.flinkRuntimeEnvironment =
                org.apache.seatunnel.core.starter.flink.execution.FlinkRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths), jobContext);

        this.sourcePluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.transformPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.sinkPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
    }

    public FlinkExecution(Config config, String jobid) {
        this.jobContext = new JobContext();
        jobContext.setJobId(jobid);
        try {
            int isRecordErrorData = config.getConfig("env").getInt("isRecordErrorData");
            int maxRecordNumber = config.getConfig("env").getInt("maxRecordNumber");
            jobContext.setIsRecordErrorData(isRecordErrorData);
            jobContext.setMaxRecordNumber(maxRecordNumber);
        } catch (Exception e) {
            log.info("");
        }
        try {
            jarPaths =
                    new ArrayList<>(
                            Collections.singletonList(
                                    new File(
                                            Common.appStarterDir()
                                                    .resolve(FlinkStarter.APP_JAR_NAME)
                                                    .toString())
                                            .toURI()
                                            .toURL()));
        } catch (MalformedURLException e) {
            throw new SeaTunnelException("load flink starter error.", e);
        }

        try {
            registerPlugin(config.getConfig("env"));
            jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));

            this.sourcePluginExecuteProcessor =
                    new SourceExecuteProcessor(
                            jarPaths, config.getConfigList(Constants.SOURCE), jobContext);
            this.transformPluginExecuteProcessor =
                    new TransformExecuteProcessor(
                            jarPaths,
                            TypesafeConfigUtils.getConfigList(
                                    config, Constants.TRANSFORM, Collections.emptyList()),
                            jobContext);
            this.sinkPluginExecuteProcessor =
                    new SinkExecuteProcessor(
                            jarPaths, config.getConfigList(Constants.SINK), jobContext);

            this.flinkRuntimeEnvironment =
                    FlinkRuntimeEnvironment.getInstance(this.registerPlugin(config, jarPaths), jobContext);
        } catch (Exception e) {
            String st_log_back_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/flinkCallBack";
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("jobId", jobid);
            objectNode.put("status", "Error");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String exceptionString = sw.toString();
            objectNode.put("error", exceptionString);
            try {
                HttpUtil.sendPostRequest(st_log_back_url, objectNode.toString());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);
        }


        this.sourcePluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.transformPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
        this.sinkPluginExecuteProcessor.setRuntimeEnvironment(flinkRuntimeEnvironment);
    }

    @Override
    public void execute() throws TaskExecuteException {
        List<DataStream<Row>> dataStreams = new ArrayList<>();
        dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);
        dataStreams = transformPluginExecuteProcessor.execute(dataStreams);
        sinkPluginExecuteProcessor.execute(dataStreams);
        log.info(
                "Flink Execution Plan: {}",
                flinkRuntimeEnvironment.getStreamExecutionEnvironment().getExecutionPlan());
        log.info("Flink job name: {}", flinkRuntimeEnvironment.getJobName());
        try {
            StreamExecutionEnvironment env = flinkRuntimeEnvironment
                    .getStreamExecutionEnvironment();
            env.registerJobListener(new MyJobListener(this.jobContext.getJobId()));
            env.getCheckpointConfig().enableUnalignedCheckpoints();
            List<JobListener> jobListeners = env.getJobListeners();
            for (JobListener jobListener : jobListeners) {
                Class<? extends JobListener> aClass = jobListener.getClass();
                System.out.println(aClass.getName());
            }
//            env.setRestartStrategy(RestartStrategies.noRestart());
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    3, // 尝试重启的次数
                    Time.of(5, TimeUnit.SECONDS) // 间隔
            ));
            env.execute(flinkRuntimeEnvironment.getJobName());

        } catch (Exception e) {
            throw new TaskExecuteException("Execute Flink job error", e);
        }
    }

    private void registerPlugin(Config envConfig) {
        List<Path> thirdPartyJars = new ArrayList<>();
        if (envConfig.hasPath(EnvCommonOptions.JARS.key())) {
            thirdPartyJars =
                    new ArrayList<>(
                            Common.getThirdPartyJars(
                                    envConfig.getString(EnvCommonOptions.JARS.key())));
        }
        thirdPartyJars.addAll(Common.getPluginsJarDependencies());
        List<URL> jarDependencies =
                Stream.concat(thirdPartyJars.stream(), Common.getLibJars().stream())
                        .map(Path::toUri)
                        .map(
                                uri -> {
                                    try {
                                        return uri.toURL();
                                    } catch (MalformedURLException e) {
                                        throw new RuntimeException(
                                                "the uri of jar illegal:" + uri, e);
                                    }
                                })
                        .collect(Collectors.toList());
        jarDependencies.forEach(
                url ->
                        FlinkAbstractPluginExecuteProcessor.ADD_URL_TO_CLASSLOADER.accept(
                                Thread.currentThread().getContextClassLoader(), url));
        jarPaths.addAll(jarDependencies);
    }

    private Config registerPlugin(Config config, List<URL> jars) {
        config =
                this.injectJarsToConfig(
                        config, ConfigUtil.joinPath("env", "pipeline", "jars"), jars);
        return this.injectJarsToConfig(
                config, ConfigUtil.joinPath("env", "pipeline", "classpaths"), jars);
    }

    private Config injectJarsToConfig(Config config, String path, List<URL> jars) {
        List<URL> validJars = new ArrayList<>();
        for (URL jarUrl : jars) {
            if (new File(jarUrl.getFile()).exists()) {
                validJars.add(jarUrl);
                log.info("Inject jar to config: {}", jarUrl);
            } else {
                log.warn("Remove invalid jar when inject jars into config: {}", jarUrl);
            }
        }

        if (config.hasPath(path)) {
            Set<URL> paths =
                    Arrays.stream(config.getString(path).split(";"))
                            .map(
                                    uri -> {
                                        try {
                                            return new URL(uri);
                                        } catch (MalformedURLException e) {
                                            throw new RuntimeException(
                                                    "the uri of jar illegal:" + uri, e);
                                        }
                                    })
                            .collect(Collectors.toSet());
            paths.addAll(validJars);

            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    paths.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));

        } else {
            config =
                    config.withValue(
                            path,
                            ConfigValueFactory.fromAnyRef(
                                    validJars.stream()
                                            .map(URL::toString)
                                            .distinct()
                                            .collect(Collectors.joining(";"))));
        }
        return config;
    }
}
