# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-module Maven project demonstrating Kafka Streams concepts and patterns, organized as examples from a Kafka Streams book. The project uses Java 21 and Kafka Streams 4.0.0 with Confluent platform components.

## Module Structure

- **Root**: Multi-module Maven project with shared dependencies
- **ch2-getting-started**: Basic Kafka Streams introduction
- **ch3-stateless-processing**: Stateless operations (filter, map, branch, etc.)
- **ch4-stateful-processing**: Stateful operations (aggregations, joins, state stores)
- **ch5-windows-and-time**: Windowing and time-based processing
- **tests**: Learning module with comprehensive test examples for various topology patterns (main module for studying Kafka Streams concepts)

## Development Commands

### Build and Compile
```bash
# Build entire project
mvn clean compile

# Build specific module
mvn clean compile -pl ch3-stateless-processing

# Package all modules
mvn clean package
```

### Testing
```bash
# Run all tests
mvn test

# Run tests for specific module
mvn test -pl tests

# Run specific test class
mvn test -Dtest=TumblingWindowTopologyTest -pl tests

# Run test with specific pattern
mvn test -Dtest="*TopologyTest" -pl tests

# Clean and test (recommended after code changes)
mvn clean test
```

### Infrastructure
```bash
# Start Kafka cluster with Schema Registry and Conduktor Console
docker-compose up -d

# Stop infrastructure
docker-compose down

# View service logs
docker-compose logs kafka
docker-compose logs schema-registry

# Reset all data (remove volumes)
docker-compose down -v
```

## Architecture and Key Patterns

### Serialization Strategy
The project demonstrates multiple serialization approaches:
- **JSON**: Custom JsonSerDes implementations in each module
- **Avro**: Schema Registry integration in ch3 with AvroSchemaAwareSerdes
- **String/Primitive**: Default Kafka serdes for simple use cases

### Testing Framework
Uses TopologyTestDriver pattern extensively:
- Unit tests for individual topologies in `tests` module
- Test utilities for input/output topic verification  
- AssertJ for fluent assertions and JUnit Jupiter for test structure
- Mockito for mocking external dependencies
- Each test class follows pattern: `@BeforeEach` setup, `@AfterEach` cleanup
- Tests use dummy bootstrap servers ("dummy-123") for isolation

### Common Topology Patterns
- **Stateless**: FilterTopology, MapTopology, BranchingTopology, MergeTopology
- **Stateful**: GroupByAndCountTopology, GroupByAndAggregateTopology, GroupByAndReduceTopology
- **Windowing**: TumblingWindowTopology, with suppression variants
- **Tables**: KTableVsKStreamTopology for stream-table duality

### State Management
- Custom state stores for complex aggregations
- JSON serialization for state store values
- Interactive queries via LeaderboardService (ch4)
- Timestamp extractors for event-time processing (ch5)

### External Integration
- Language detection client abstraction (ch3)
- REST API endpoints using Javalin (ch4)
- Patient monitoring with vital signs processing (ch5)

## Development Notes

- All modules inherit from root POM with shared Kafka Streams dependencies
- Java 21 is required (configured in maven-compiler-plugin)
- Each chapter focuses on specific Kafka Streams concepts
- The `tests` module is the primary learning module containing isolated examples perfect for studying Kafka Streams patterns and concepts
- Docker Compose provides complete Kafka development environment
- Uses Conduktor Console for Kafka cluster visualization (localhost:8080)
- Schema Registry runs on localhost:8081 for Avro serialization examples

## Key Services and URLs

- **Conduktor Console**: http://localhost:8080 (Kafka cluster management)
- **Schema Registry**: http://localhost:8081 (Avro schema management)
- **Kafka Bootstrap**: localhost:9092 (client connections)