package tutorial.storm.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.spout.IBatchSpout;
import tutorial.storm.trident.operations.FilterByRegex;
import tutorial.storm.trident.operations.ToUpperCase;
import tutorial.storm.trident.testutil.FakeTweetsBatchSpout;

import java.io.IOException;


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
public class Lesson01_BasicPrimitives {
    private static final Logger log = LoggerFactory.getLogger(Lesson01_BasicPrimitives.class);

    public static void main(String[] args) throws Exception {
        // Storm can be run locally for testing purposes
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("basic_primitives", conf, basicPrimitives(new FakeTweetsBatchSpout()));
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

        // The "each" primitive allows us to apply either filters or functions to the stream
        // We always have to select the input fields.
        topology
                .newStream("filter", spout)
                .each(new Fields("actor"), new FilterByRegex("pere"))
                .each(new Fields("text", "actor"), new Debug());

        // Functions describe their output fields, which are always appended to the input fields.
        topology
                .newStream("function", spout)
                .each(new Fields("text", "actor"), new ToUpperCase(), new Fields("uppercased_text"))
                .each(new Fields("text", "uppercased_text"), new Debug());

        // As you see, Each operations can be chained.

        return topology.build();
    }

}
