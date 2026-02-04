# Lab 5: Mini-MapReduce on Amazon EMR

## Dataset
Wikipedia Simple English corpus from:
https://github.com/LGDoor/Dump-of-Simple-English-Wiki

## Files
- `mapper.py`: Map phase - tokenizes text and emits word counts
- `reducer.py`: Reduce phase - aggregates word counts

## Run Command
```bash
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -input /user/hadoop/input/corpus.txt \
  -output /user/hadoop/output/wordcount \
  -mapper mapper.py \
  -reducer reducer.py \
  -files mapper.py,reducer.py
```
