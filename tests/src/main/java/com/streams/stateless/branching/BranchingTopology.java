package com.streams.stateless.branching;

import com.streams.common.model.MyModel;
import com.streams.common.serde.JsonSerDes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
 * Tests BranchingTopology method: split() with branch() and defaultBranch()
 * 
 * split() - divides stream into multiple branches based on predicates
 * Each record goes to first matching branch or default if none match
 */
public class BranchingTopology {
    private static final Logger logger = LoggerFactory.getLogger(BranchingTopology.class);

    /**
     * Creates topology that splits stream based on name patterns.
     * 
     * BRANCHING LOGIC:
     * - uppercase: names starting with uppercase letter ^[A-Z].*
     * - lowercase: names starting with lowercase letter ^[a-z].*  
     * - default: everything else (numbers, symbols, etc.)
     */
    public static Topology createBranchingTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MyModel> stream = builder.stream(
                "input-topic",
                Consumed.with(Serdes.String(), new JsonSerDes<>(MyModel.class))
        );

        // Define predicates for branching logic
        Predicate<String, MyModel> uppercasePredicate = (key, myModel) ->
            myModel.getName() != null && myModel.getName().matches("^[A-Z].*");

        Predicate<String, MyModel> lowercasePredicate = (key, myModel) ->
            myModel.getName() != null && myModel.getName().matches("^[a-z].*");

        // define branching
        Map<String, KStream<String, MyModel>> branchesMap = stream.split(Named.as("branch-"))
                .branch(uppercasePredicate, Branched.as("uppercase"))
                .branch(lowercasePredicate, Branched.as("lowercase"))
                .defaultBranch(Branched.as("default-upper-lower-case-does-not-apply"));

        // Extract individual branches from map
        KStream<String, MyModel> uppercaseBranch = branchesMap.get("branch-uppercase");
        KStream<String, MyModel> lowercaseBranch = branchesMap.get("branch-lowercase");
        KStream<String, MyModel> defaultBranch = branchesMap.get("branch-default-upper-lower-case-does-not-apply");

        // Debug logging for each branch
        uppercaseBranch.peek((key, value) -> logger.info("uppercase branch (key, value): (" + key + ", " + value + ")") );
        lowercaseBranch.peek((key, value) -> logger.info("lowercase branch (key, value): (" + key + ", " + value + ")") );
        defaultBranch.peek((key, value) -> logger.info("default branch (key, value): (" + key + ", " + value + ")") );

        // Route each branch to different output topics
        uppercaseBranch.to("output-topic-uppercase");
        lowercaseBranch.to("output-topic-lowercase");
        defaultBranch.to("output-topic-default");

        return builder.build();
    };
}
