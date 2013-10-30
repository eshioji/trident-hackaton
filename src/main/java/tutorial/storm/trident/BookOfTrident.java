package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.DRPCClient;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.MapUtils;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.FeederBatchSpout;
import trident.memcached.MemcachedState;
import tutorial.storm.trident.operations.FilterByRegex;
import tutorial.storm.trident.operations.Split;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.*;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import tutorial.storm.trident.testutil.FakeTweetGenerator;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;
import tutorial.storm.trident.testutil.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is not meant to be run, instead it is meant to be read.
 *
 * If you want to run some stream you'll need to comment out everything else. Otherwise the topology will run all the
 * streams at the same time, which can be a bit of a chaos.
 *
 * @author pere
 * Modified by @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class BookOfTrident {

    public static void main(String[] args) throws Exception {
        FakeTweetGenerator fakeTweets = new FakeTweetGenerator();

        // Storm can be run locally for testing purposes
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();


        // Learn about Trident's basic primitives
        cluster.submitTopology("basic_primitives", conf, basicPrimitives(new FakeTweetsBatchSpout()));


        // Learn how to use Trident State and DRPC
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("id", "text", "actor", "location", "date"));
        LocalDRPC drpc = new LocalDRPC();
        cluster.submitTopology("state_drpc", conf, stateAndDRPC(drpc, testSpout));

        // You can use FeederBatchSpout to feed know values to the topology. Very useful for tests.
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("ted"));
        testSpout.feed(fakeTweets.getNextTweetTuples("mary"));
        testSpout.feed(fakeTweets.getNextTweetTuples("jason"));

        // This is how you make DRPC calls. First argument must match the function name
        System.out.println(drpc.execute("ping", "ping pang pong"));
        System.out.println(drpc.execute("count", "america america ace ace ace item"));
        System.out.println(drpc.execute("count_per_actor", "ted"));
        System.out.println(drpc.execute("count_per_actors", "ted mary pere jason"));

        // You can use a client library to make calls remotely
//        DRPCClient client = new DRPCClient("drpc.server.location", 3772);
//        System.out.println(client.execute("ping", "ping pang pong"));

    }


    public static StormTopology basicPrimitives(IBatchSpout spout) throws IOException {

        // A topology is a set of streams.
        // A stream is a DAG of Spouts and Bolts.
        // (In Storm there are Spouts (data producers) and Bolts (data processors).
        // Spouts create Tuples and Bolts manipulate then and possibly emit new ones.)

        // But in Trident we operate at a higher level.
        // Bolts are created and connected automatically out of higher-level constructs.
        // Also, Spouts are "batched".
        TridentTopology topology = new TridentTopology();

        // Each primitive allows us to apply either filters or functions to the stream
        // We always have to select the input fields.
        topology.newStream("filter", spout).each(new Fields("text", "actor"), new PereTweetsFilter())
                .each(new Fields("text", "actor"), new Utils.PrintFilter());

        // Functions describe their output fields, which are always appended to the input fields.
        topology
                .newStream("function", spout)
                .each(new Fields("text", "actor"), new UppercaseFunction(), new Fields("uppercased_text"))
                .each(new Fields("text", "uppercased_text"), new Utils.PrintFilter());

        // As you see, Each operations can be chained.

        // Stream can be parallelized with "parallelismHint"
        // Parallelism hint is applied downwards until a partitioning operation (we will see this later).
        // This topology creates 5 spouts and 5 bolts:
        // Let's debug that with TridentOperationContext . partitionIndex !
        topology
                .newStream("parallel", spout)
                .each(new Fields("text", "actor"), new PereTweetsFilter())
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new Utils.PrintFilter());

        // A stream can be partitioned in various ways.
        // Let's partition it by "actor". What happens with previous example?
        topology
                .newStream("parallel_and_partitioned", spout)
                .partitionBy(new Fields("actor"))
                .each(new Fields("text", "actor"), new PereTweetsFilter())
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new Utils.PrintFilter());

        // Only one partition is filtering, which makes sense for the case.
        // If we remove the partitionBy we get the previous behavior.

        // Before we have parallelism = 5 everywhere. What if we want only one spout?
        // We need to specify a partitioning policy for that to happen.
        // (We said that parallelism hint is applied downwards until a partitioning operation is found).

        // But if we don't want to partition by any field, we can just use shuffle()
        // We could also choose global() - with care!
        topology
                .newStream("parallel_and_partitioned", spout)
                .parallelismHint(1)
                .shuffle()
                .each(new Fields("text", "actor"), new PereTweetsFilter())
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new Utils.PrintFilter());

        // Because data is batched, we can aggregate batches for efficiency.
        // The aggregate primitive aggregates one full batch. Useful if we want to persist the result of each batch only
        // once.
        // The aggregation for each batch is executed in a random partition as can be seen:
        topology
                .newStream("aggregation", spout)
                .parallelismHint(1)
                .aggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
                .parallelismHint(5)
                .each(new Fields("aggregated_result"), new Utils.PrintFilter());

        // The partitionAggregate on the other hand only executes the aggregator within one partition's part of the batch.
        // Let's debug that with TridentOperationContext . partitionIndex !
        topology
                .newStream("partial_aggregation", spout)
                .parallelismHint(1)
                .shuffle()
                .partitionAggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
                .parallelismHint(6)
                .each(new Fields("aggregated_result"), new Utils.PrintFilter());

        // (See what happens when we change the Spout batch size / parallelism)

        // A useful primitive is groupBy.
        // It splits the stream into groups so that aggregations only ocurr within a group.
        // Because now we are grouping, the aggregation function can be much simpler (Count())
        // We don't need to use HashMaps anymore.
        topology
                .newStream("aggregation", spout)
                .parallelismHint(1)
                .groupBy(new Fields("location"))
                .aggregate(new Fields("location"), new Count(), new Fields("count"))
                .parallelismHint(5)
                .each(new Fields("location", "count"), new Utils.PrintFilter());

        // EXERCISE: Use Functions and Aggregators to parallelize per-hashtag counts.
        // Step by step: 1) Obtain and select hashtags, 2) Write the Aggregator.

        // Bonus : "Trending" hashtags.
        return topology.build();
    }


    private static StormTopology stateAndDRPC(LocalDRPC drpc, FeederBatchSpout spout) throws IOException {
        TridentTopology topology = new TridentTopology();

        // persistentAggregate persists the result of aggregation into data stores,
        // which you can use from other applications.
        // You can also use it in other topologies by using the TridentState object returned.
        //
        // The state is commonly backed by a data store like memcache, cassandra etc.
        // Here we are simply using a hash map
        TridentState countState =
                topology
                        .newStream("spout", spout)
                        .groupBy(new Fields("actor"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        // There are a few ready-made state libraries that you can use
        // Below is an example to use memcached
//        List<InetSocketAddress> memcachedServerLocations = ImmutableList.of(new InetSocketAddress("some.memcached.server",12000));
//        TridentState countStateMemcached =
//                topology
//                        .newStream("spout", spout)
//                        .groupBy(new Fields("actor"))
//                        .persistentAggregate(MemcachedState.transactional(memcachedServerLocations), new Count(), new Fields("count"));



        // DRPC stands for Distributed Remote Procedure Call
        // You can issue calls using the DRPC client library
        // A DRPC call takes two Strings, function name and function arguments
        //
        // In order to call the DRPC defined below, you'd use "count_per_actor" as the function name
        // The function arguments will be available as "args"
        topology
                .newDRPCStream("ping", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("reply"))
                .each(new Fields("reply"), new FilterByRegex("ping"))
                .project(new Fields("reply"));

        // You can apply usual processing primitives to DRPC streams as well
        topology
                .newDRPCStream("count", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("split"))
                .each(new Fields("split"), new FilterByRegex("a.*"))
                .groupBy(new Fields("split"))
                .aggregate(new Count(), new Fields("count"));


        // More usefully, you can query the state you created earlier
        topology
                .newDRPCStream("count_per_actor", drpc)
                .stateQuery(countState, new Fields("args"), new MapGet(), new Fields("count"));

        // Here is a more complex example
        topology
                .newDRPCStream("count_per_actors", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("actor"))
                .groupBy(new Fields("actor"))
                .stateQuery(countState, new Fields("actor"), new MapGet(), new Fields("individual_count"))
                .each(new Fields("individual_count"), new FilterNull())
                .aggregate(new Fields("individual_count"), new Sum(), new Fields("count"));

        // For how to call DRPC calls, go back to the main method

        return topology.build();
    }


    /**
     * Dummy filter that just keeps tweets by "Pere"
     */
    @SuppressWarnings({"serial", "rawtypes"})
    public static class PereTweetsFilter implements Filter {

        int partitionIndex;

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            this.partitionIndex = context.getPartitionIndex();
        }

        @Override
        public void cleanup() {
        }

        @Override
        public boolean isKeep(TridentTuple tuple) {
            boolean filter = tuple.getString(1).equals("pere");
            if (filter) {
                System.err.println("I am partition [" + partitionIndex + "] and I have filtered pere.");
            }
            return filter;
        }
    }

    /**
     * Dummy function that just emits the uppercased tweet text.
     */
    @SuppressWarnings("serial")
    public static class UppercaseFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            collector.emit(new Values(tuple.getString(0).toUpperCase()));
        }
    }

    /**
     * A simple Aggregator that produces a hashmap of key, counts.
     */
    @SuppressWarnings("serial")
    public static class LocationAggregator implements Aggregator<Map<String, Integer>> {

        int partitionId;

        @SuppressWarnings("rawtypes")
        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            this.partitionId = context.getPartitionIndex();
        }

        @Override
        public void cleanup() {
        }

        @Override
        public Map<String, Integer> init(Object batchId, TridentCollector collector) {
            return new HashMap<String, Integer>();
        }

        @Override
        public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
            String loc = tuple.getString(0);
            val.put(loc, MapUtils.getInteger(val, loc, 0) + 1);
        }

        @Override
        public void complete(Map<String, Integer> val, TridentCollector collector) {
            System.err.println("I am partition [" + partitionId + "] and have aggregated: [" + val + "]");
            collector.emit(new Values(val));
        }
    }

}
