## Manual Test
docker compose copies test data from the data directory to kafka container.

So, to test the application:
1. docker compose up
2. docker exec -it kafka bash

and then produce the data:

## produce test data to players topic
kafka-console-producer \
--bootstrap-server kafka:9092 \
--topic players \
--property 'parse.key=true' \
--property 'key.separator=|' < players.json

## produce test data to products topic
kafka-console-producer \
--bootstrap-server kafka:9092 \
--topic products \
--property 'parse.key=true' \
--property 'key.separator=|' < products.json

## produce test data to score-events topic
kafka-console-producer \
--bootstrap-server kafka:9092 \
--topic score-events < score-events.json
```

```json
 [score-events]             [players]                  [products]
 key = null                 key = playerId             key = productId
 value = ScoreEvent         value = Player             value = Product
        |                         |                          |
        | selectKey(playerId)     |                          |
        v                         |                          |
 key = playerId                   |                          |
 value = ScoreEvent               |                          |
        |                         |                          |
        +----------- join on playerId ---------+              |
                                                v             |
                                       [ScoreWithPlayer]      |
                                       key = playerId         |
                                       value = {ScoreEvent+Player}
                                                |             |
                                                | join on productId
                                                +-------------+
                                                              v
                                                       [Enriched]
                                                       key = playerId
                                                       value = {playerId, productId,
                                                                playerName, gameName, score}
                                                              |
                                                              | groupBy(productId)
                                                              v
                                                [repartition topic]
                                                key = productId
                                                value = Enriched
                                                              |
                                                              | aggregate
                                                              v
                                                     [HighScores KTable]
                                                     key = productId
                                                     value = HighScores
                                                     (store: leader-boards)

```

## When resetting docker
Kafka Streams has been configured to save the RocksDB data at /tmp.  
Delete the streams-app-1, streams-app-2 files when resetting the containers to prevent errors.

```
stodo@dawid:/tmp$ rm -rf streams-app-2   
stodo@dawid:/tmp$ rm -rf streams-app-1
stodo@dawid:/tmp$ pwd
/tmp
```  
