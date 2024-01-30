package org.apache.seatunnel.core.starter.flink.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.seatunnel.core.starter.flink.utils.HttpUtil;

import javax.annotation.Nullable;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

@Slf4j
public class MyJobListener implements JobListener, Serializable {

    private String jobId;

    public MyJobListener(String jobId) {
        this.jobId = jobId;
    }

    private static final long serialVersionUID = -4648179264065868285L;

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        String st_log_back_url = System.getenv("ST_SERVICE_URL")+"/SeaTunnelJob/flinkCallBack";
        if (throwable == null) {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put("jobId", jobExecutionResult.getJobID().toHexString());
                objectNode.put("status", "FINISHED");
                objectNode.put("runtime",jobExecutionResult.getNetRuntime());
                HttpUtil.sendPostRequest(st_log_back_url, objectNode.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                ObjectMapper mapper = new ObjectMapper();
                ObjectNode objectNode = mapper.createObjectNode();
                objectNode.put("jobId", this.jobId);
                objectNode.put("status", "FAILED");
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                throwable.printStackTrace(pw);
                String exceptionString = sw.toString();
                objectNode.put("error", exceptionString);
                HttpUtil.sendPostRequest(st_log_back_url, objectNode.toString());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
