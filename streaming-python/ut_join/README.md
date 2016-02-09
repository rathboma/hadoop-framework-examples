To run

upload files:

```bash
hadoop fs -mkdir transactions
hadoop fs -mkdir users
hadoop fs -put users.txt users/
hadoop fs -put transactions.txt transactions/

```

1st step (join):
hadoop jar ./contrib/streaming/hadoop-0.20.2-streaming.jar -Dmapred.reduce.tasks=1 -Dstream.num.map.output.key.fields=2 -input transactions -input users -output transactions_and_users_output -file /path/to/joinMapperTU.py -file /path/to/joinReducerTU.py -mapper joinMapperTU.py -reducer joinReducerTU.py

2nd step (count distinct by key):
hadoop jar ./contrib/streaming/hadoop-0.20.2-streaming.jar -Dmapred.reduce.tasks=1 -Dstream.num.map.output.key.fields=2 -input transactions_and_users_output -output transactions_and_users_output_final -file /path/to/joinMapperTU1.py -file /path/to/joinReducerTU1.py -mapper joinMapperTU1.py -reducer joinReducerTU1.py
