/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.util.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

// Commons Logging imports

public class Crawl extends NutchTool implements Tool {
    private static final Logger LOG = LoggerFactory
            .getLogger(MethodHandles.lookup().lookupClass());
    private ObjectMapper mapper = new ObjectMapper();

    private static String getDate() {
        return new SimpleDateFormat("yyyyMMddHHmmss").format
                (new Date(System.currentTimeMillis()));
    }

    /* Perform complete crawling and indexing (to Solr) given a set of root urls and the -solr
       parameter respectively. More information and Usage parameters can be found below. */
    public static void main(String args[]) throws Exception {
        Configuration conf = NutchConfiguration.create();
        int res = ToolRunner.run(conf, new Crawl(), args);
        System.exit(res);
    }

    /**
     * Used by the Nutch REST service
     */
    public Map<String, Object> run(Map<String, Object> args, String crawlId)
            throws Exception {
        if (args.size() < 1) {
            throw new IllegalArgumentException("Required arguments <url_dir> or <seedName>");
        }
        LOG.info("Injector: args: " + mapper.writeValueAsString(args));
        Path input;
        Object path = null;
        if (args.containsKey(Nutch.ARG_SEEDDIR)) {
            path = args.get(Nutch.ARG_SEEDDIR);
        } else if (args.containsKey(Nutch.ARG_SEEDNAME)) {
            path = NutchServer.getInstance().getSeedManager().
                    getSeedList((String) args.get(Nutch.ARG_SEEDNAME)).getSeedFilePath();
        } else {
            throw new IllegalArgumentException("Required arguments <url_dir> or <seedName>");
        }
        if (path instanceof Path) {
            input = (Path) path;
        } else {
            input = new Path(path.toString());
        }
        Map<String, Object> results = new HashMap<>();
        Path crawlDb;
        if (args.containsKey(Nutch.ARG_CRAWLDB)) {
            Object crawldbPath = args.get(Nutch.ARG_CRAWLDB);
            if (crawldbPath instanceof Path) {
                crawlDb = (Path) crawldbPath;
            } else {
                crawlDb = new Path(crawldbPath.toString());
            }
        } else {
            crawlDb = new Path(crawlId + "/crawldb");
        }
        int threads = getConf().getInt("fetcher.threads.fetch", 10);
        int depth = 5;
        int loop = 1;
        long topN = Long.MAX_VALUE;
        if (args.containsKey(Nutch.ARG_THREADS)) {
            threads = Integer.parseInt(args.get(Nutch.ARG_THREADS).toString());
        }
        if (args.containsKey(Nutch.ARG_DEPTH)) {
            depth = Integer.parseInt(args.get(Nutch.ARG_DEPTH).toString());
        }
        if (args.containsKey(Nutch.ARG_TOPN)) {
            topN = Integer.parseInt(args.get(Nutch.ARG_TOPN).toString());
        }
        crawl(input, crawlDb, threads, depth, topN);
        results.put(Nutch.VAL_RESULT, Integer.toString(0));
        return results;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println
                    ("Usage: Crawl <urlDir> -solr <solrURL> [-dir d] [-threads n] [-depth i] [-topN N]");
            return -1;
        }
        Path rootUrlDir = null;
        Path dir = new Path("crawl-" + getDate());
        int threads = getConf().getInt("fetcher.threads.fetch", 10);
        int depth = 5;
        int loop = 1;
        long topN = Long.MAX_VALUE;

        for (int i = 0; i < args.length; i++) {
            if ("-dir".equals(args[i])) {
                dir = new Path(args[i + 1]);
                i++;
            } else if ("-threads".equals(args[i])) {
                threads = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-depth".equals(args[i])) {
                depth = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-topN".equals(args[i])) {
                topN = Integer.parseInt(args[i + 1]);
                i++;
            } else if (args[i] != null) {
                rootUrlDir = new Path(args[i]);
            }
        }

        return crawl(rootUrlDir, dir, threads, depth, topN);
    }

    private int crawl(Path rootUrlDir, Path dir, int threads, int depth, long topN) throws IOException, ClassNotFoundException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();

        Job job = NutchJob.getInstance(getConf());
        FileSystem fs = FileSystem.get(job.getConfiguration());

        if (LOG.isInfoEnabled()) {
            LOG.info("Crawl: Started in: " + dir);
            LOG.info("Crawl: rootUrlDir = " + rootUrlDir);
            LOG.info("Crawl: threads = " + threads);
            LOG.info("Crawl: depth = " + depth);
            // LOG.info("solrUrl=" + solrUrl);
            if (topN != Long.MAX_VALUE)
                LOG.info("Crawl: topN = " + topN);
        }

        Path inputDb = new Path(dir + "/inputdb");
        Path crawlDb = new Path(dir + "/crawldb");
        Path linkDb = new Path(dir + "/linkdb");
        Path segments = new Path(dir + "/segments");

        Injector injector = new Injector(getConf());
        Generator generator = new Generator(getConf());
        Fetcher fetcher = new Fetcher(getConf());
        ParseSegment parseSegment = new ParseSegment(getConf());
        CrawlDb crawlDbTool = new CrawlDb(getConf());
        LinkDb linkDbTool = new LinkDb(getConf());
        DeduplicationJob deduplicationJob = new DeduplicationJob();

        Path[] segs = null;
        while(true) {
            fs.delete(inputDb, true);
            injector.inject(inputDb, rootUrlDir);
            segs = generator.generate(inputDb, segments, -1, topN, System.currentTimeMillis());
            if (segs != null) {
                fetcher.fetch(segs[0], threads);  // fetch it
                if (!Fetcher.isParsing(job.getConfiguration())) {
                    parseSegment.parse(segs[0]);    // parse it, if needed
                }
                crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
            } else {
                LOG.info("Crawl: Stopping inject to inputDB - no more URLs to fetch.");
                break;
            }
            int i;
            for (i = 0; i < depth; i++) {           // generate new segment
                segs = generator.generate(crawlDb, segments, -1, topN, System
                        .currentTimeMillis());
                if (segs == null) {
                    LOG.info("Crawl: Stopping at depth = " + i + " - no more URLs to fetch.");
                    break;
                }
                fetcher.fetch(segs[0], threads);  // fetch it
                if (!Fetcher.isParsing(job.getConfiguration())) {
                    parseSegment.parse(segs[0]);    // parse it, if needed
                }
                crawlDbTool.update(crawlDb, segs, true, true); // update crawldb
            }
            if (i > 0) {
                try {
                    linkDbTool.invert(linkDb, segments, true, true, false); // invert links
                } catch (Exception ex) {
                    LOG.warn("Crawl: Errors when invert links: " + ex.toString());
                }
            } else {
                LOG.warn("Crawl: No URLs to fetch - check your seed list and URL filters.");
            }
            // deduplicationJob.dedup(crawlDb);
        }
        //Delete segments temp
        FileStatus[] files = fs.listStatus(segments, HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] sPaths = HadoopFSUtil.getPaths(files);
        for (Path path : sPaths) {
            try {
                fs.delete(path, true);
            } catch (Exception ex) {
            }
        }

        long end = System.currentTimeMillis();
        LOG.info("Crawl: Finished at " + sdf.format(end) + ", elapsed: "
                + TimingUtil.elapsedTime(start, end));
        return 0;
    }


}