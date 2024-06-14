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

package org.apache.seatunnel.example.flink.v2;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.flink.args.FlinkCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.UUID;

public class SeaTunnelApiExample {

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        //本地json文件执行
        runLocal(args);
        //从数据库获取配置文件执行
//            runFromDb(args);

    }

    public static void runLocal(String[] args)  throws FileNotFoundException, URISyntaxException, CommandException{
        String configurePath = args.length > 0 ? args[0] : "/examples/a4.json";
        String configFile = getTestConfigFile(configurePath);
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }

    public static  void runFromDb(String[] args) throws FileNotFoundException, URISyntaxException, CommandException{
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setDb(true);
        flinkCommandArgs.setConfigFile("1798596688309436417");
//        flinkCommandArgs.setConfigFile("1770613255461834753");
        String fid = UUID.randomUUID().toString().replaceAll("-", "");
        System.out.println("---------------------------------------fid---------------------------" + fid);
        flinkCommandArgs.setFlinkJobId(fid);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        SeaTunnel.run(flinkCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelApiExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
