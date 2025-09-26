### Add VM options in intellij to start locally:
```
-Dserver.port=8085 -Dstate.dir=/tmp/streams-app-1
```

## Manual Test
docker compose copies test data from the data directory to kafka container.

So, to test the application:
1. docker compose up
2. docker exec -it kafka bash

and then produce the data:

## produce body-temp-events
kafka-console-producer \
--bootstrap-server kafka:9092 \
--topic body-temp-events \
--property 'parse.key=true' \
--property 'key.separator=|' < body-temp-events.json

## produce pulse-events
kafka-console-producer \
--bootstrap-server kafka:9092 \
--topic pulse-events \
--property 'parse.key=true' \
--property 'key.separator=|' < pulse-events.json

