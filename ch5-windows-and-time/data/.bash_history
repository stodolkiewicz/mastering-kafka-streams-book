ls
kafka-console-producer --bootstrap-server kafka:9092 --topic body-temp-events --property 'parse.key=true' --property 'key.separator=|' < body-temp-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic pulse-events --property 'parse.key=true' --property 'key.separator=|' < pulse-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic body-temp-events --property 'parse.key=true' --property 'key.separator=|' < body-temp-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic pulse-events --property 'parse.key=true' --property 'key.separator=|' < pulse-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic body-temp-events --property 'parse.key=true' --property 'key.separator=|' < body-temp-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic body-temp-events --property 'parse.key=true' --property 'key.separator=|' < body-temp-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic pulse-events --property 'parse.key=true' --property 'key.separator=|' < pulse-events.json
kafka-console-producer --bootstrap-server kafka:9092 --topic pulse-events --property 'parse.key=true' --property 'key.separator=|' < pulse-events.json
exit
