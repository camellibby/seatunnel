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

package org.apache.seatunnel.core.starter.flink.command;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.starter.flink.execution.FlinkExecution;
import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;
import org.apache.seatunnel.shade.com.typesafe.config.*;

import java.sql.*;
import java.util.Properties;

@Slf4j
public class FlinkTaskFromDbExecuteCommand implements Command<FlinkCommandArgs> {

    private final FlinkCommandArgs flinkCommandArgs;

    public FlinkTaskFromDbExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        Config config = ConfigFactory.parseString(getConfig(), ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(
                        ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
        config = ConfigShadeUtils.decryptConfig(config);

        // if user specified job name using command line arguments, override config option
        if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
            config =
                    config.withValue(
                            ConfigUtil.joinPath("env", "job.name"),
                            ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
        }
        FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config);
        try {
            seaTunnelTaskExecution.execute();
        } catch (Exception e) {
            throw new CommandExecuteException("Flink job executed failed", e);
        }
    }

    private String getConfig() {
        String user = System.getenv("MYSQL_MASTER_USER");
        String password = System.getenv("MYSQL_MASTER_PWD");
        String dbHost = System.getenv("MYSQL_MASTER_HOST");
        String dbPort = System.getenv("MYSQL_MASTER_PORT");
        String dbName = System.getenv("PANGU_DB");
        String url = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName;
        String sql = "select job_config from seatunnel_jobs where id=?";
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        String config = null;
        try (Connection connection = new com.mysql.cj.jdbc.Driver().connect(url, info);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, NumberUtils.toInt(flinkCommandArgs.getConfigFile()));
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                config = resultSet.getString("job_config");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info(config);
        return config;
    }
}
