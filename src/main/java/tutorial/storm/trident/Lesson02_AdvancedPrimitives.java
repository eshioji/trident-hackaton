package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import tutorial.storm.trident.operations.FilterByRegex;
import tutorial.storm.trident.operations.LocationAggregator;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Lesson02_AdvancedPrimitives {
    private static final Logger log = LoggerFactory.getLogger(Lesson02_AdvancedPrimitives.class);

    public static void main(String[] args) throws Exception {
        // Storm can be run locally for testing purposes
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("advanced_primitives", conf, advancedPrimitives(new FakeTweetsBatchSpout()));
    }

    private static StormTopology advancedPrimitives(FakeTweetsBatchSpout spout) {

        TridentTopology topology = new TridentTopology();



        // A stream can be partitioned in various ways.
        // Let's partition it by "actor". What happens with previous example?
        topology
                .newStream("parallel_and_partitioned", spout)
                .partitionBy(new Fields("actor"))
                .each(new Fields("text", "actor"), new FilterByRegex("pere"))
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new Debug());

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
                .each(new Fields("text", "actor"), new FilterByRegex("pere"))
                .parallelismHint(5)
                .each(new Fields("text", "actor"), new Debug());

        // Because data is batched, we can aggregate batches for efficiency.
        // The aggregate primitive aggregates one full batch. Useful if we want to persist the result of each batch only
        // once.
        // The aggregation for each batch is executed in a random partition as can be seen:
        topology
                .newStream("aggregation", spout)
                .parallelismHint(1)
                .aggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
                .parallelismHint(5)
                .each(new Fields("aggregated_result"), new Debug());

        // The partitionAggregate on the other hand only executes the aggregator within one partition's part of the batch.
        // Let's debug that with TridentOperationContext . partitionIndex !
        topology
                .newStream("partial_aggregation", spout)
                .parallelismHint(1)
                .shuffle()
                .partitionAggregate(new Fields("location"), new LocationAggregator(), new Fields("aggregated_result"))
                .parallelismHint(6)
                .each(new Fields("aggregated_result"), new Debug());

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
                .each(new Fields("location", "count"), new Debug());

        // EXERCISE: Use Functions and Aggregators to parallelize per-hashtag counts.
        // Step by step: 1) Obtain and select hashtags, 2) Write the Aggregator.

        // Bonus : "Trending" hashtags.
        return topology.build();
    }

}
