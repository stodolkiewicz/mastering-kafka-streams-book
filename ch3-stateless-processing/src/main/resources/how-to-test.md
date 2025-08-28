### Copy test files to kafka container 
user@DESKTOP-TA8MITE MINGW64 ~/Desktop/kafka/my-kafka-streams/ch2-getting-started/src/main/resources (main)
$ docker cp test1.json kafka:/home/appuser/test1.json
Successfully copied 2.05kB to kafka:/home/appuser/test1.json

user@DESKTOP-TA8MITE MINGW64 ~/Desktop/kafka/my-kafka-streams/ch2-getting-started/src/main/resources (main)
$ docker cp test1.json kafka:/home/appuser/test2.json
Successfully copied 2.05kB to kafka:/home/appuser/test2.json

### Produce messages
[appuser@kafka ~]$ kafka-console-producer --bootstrap-server kafka:9092 --topic tweets < test1.json
[appuser@kafka ~]$ kafka-console-producer --bootstrap-server kafka:9092 --topic tweets < test2.json

### Read messages
kafka-console-consumer --bootstrap-server kafka:9092 --topic crypto-sentiment  --from-beginning