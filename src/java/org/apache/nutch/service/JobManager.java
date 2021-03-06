/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.service;

import java.util.Collection;
import org.apache.nutch.service.model.request.JobConfig;
import org.apache.nutch.service.model.response.JobInfo;
import org.apache.nutch.service.model.response.JobInfo.State;

public interface JobManager {

  public static enum JobType{
    CRAWL, INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INDEX, READDB, CLASS, INVERTLINKS, DEDUP
  };
  public Collection<JobInfo> list(String crawlId, State state);

  public JobInfo get(String crawlId, String id);

  /**
   * Creates specified job
   * @param jobConfig
   * @return JobInfo
   */
  public JobInfo create(JobConfig jobConfig);

  public boolean abort(String crawlId, String id);

  public boolean stop(String crawlId, String id);
}
