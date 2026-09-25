# Hadoop HDFS + MapReduce – Word Count Use Case

## Use Case

Count how many times each word appears in a text file using HDFS and MapReduce with Python.

## Technologies

1. Hadoop HDFS – Stores input and output data
2. Hadoop MapReduce – Processes the data
3. Python – Mapper and Reducer
4. Hadoop Streaming – Runs Python MapReduce programs

## 1. Project Flow

Local File
    ↓
HDFS Input
    ↓
Mapper
    ↓
(word, 1)
    ↓
Shuffle and Sort
    ↓
(word, [1,1,1,...])
    ↓
Reducer
    ↓
(word, total_count)
    ↓
HDFS Output

## 2. Create Local Project Directory

    mkdir ~/wordcount
    cd ~/wordcount

Check current path:

    pwd

Expected:

    /home/ganesh/wordcount

## 3. Create Input File

    nano input.txt

Enter:

    Hadoop is good
    Hadoop is powerful
    Hadoop is used for big data
    Big data is powerful
    Hadoop is good for big data

Save:

    Ctrl + O
    Enter
    Ctrl + X

Check the file:

    cat input.txt

## 4. Create HDFS Input Directory

Check HDFS root:

    hdfs dfs -ls /

Create the input directory:

    hdfs dfs -mkdir -p /wordcount/input

Important:

/wordcount/input is an HDFS directory.
It is not the same as ~/wordcount/input on the local Linux filesystem.

## 5. Upload Input File to HDFS

    hdfs dfs -put input.txt /wordcount/input/

Check:

    hdfs dfs -ls /wordcount/input

Display the file from HDFS:

    hdfs dfs -cat /wordcount/input/input.txt

## 6. Create Mapper

    nano mapper.py

Code:

    #!/usr/bin/env python3

    import sys

    for line in sys.stdin:
        words = line.strip().lower().split()

        for word in words:
            print(f"{word}\t1")

Save:

    Ctrl + O
    Enter
    Ctrl + X

## 7. How Mapper Works

Input:

    Hadoop is good

Mapper output:

    hadoop    1
    is        1
    good      1

Input:

    Hadoop is powerful

Mapper output:

    hadoop    1
    is        1
    powerful  1

The mapper converts every word into:

    (word, 1)

## 8. Create Reducer

    nano reducer.py

Code:

    #!/usr/bin/env python3

    import sys

    current_word = None
    current_count = 0

    for line in sys.stdin:
        word, count = line.strip().split("\t")
        count = int(count)

        if current_word == word:
            current_count += count
        else:
            if current_word is not None:
                print(f"{current_word}\t{current_count}")

            current_word = word
            current_count = count

    if current_word is not None:
        print(f"{current_word}\t{current_count}")

Save:

    Ctrl + O
    Enter
    Ctrl + X

## 9. How Reducer Works

Mapper may produce:

    hadoop    1
    is        1
    good      1
    hadoop    1
    is        1
    powerful  1

During Shuffle and Sort, Hadoop groups the same words:

    hadoop    [1,1]
    good      [1]
    is        [1,1]
    powerful  [1]

Reducer adds the values:

    hadoop    2
    good      1
    is        2
    powerful  1

## 10. Test Mapper Locally

Run:

    cat input.txt | python3 mapper.py

Expected type of output:

    hadoop    1
    is        1
    good      1
    hadoop    1
    is        1
    powerful  1

## 11. Test Mapper and Reducer Locally

Run:

    cat input.txt | python3 mapper.py | sort | python3 reducer.py

This simulates:

    Mapper
    ↓
    Sort
    ↓
    Reducer

Expected output:

    big       3
    data      3
    for       2
    good      2
    hadoop    4
    is        4
    powerful  2
    used      1

The exact counts depend on the input.

## 12. Check Hadoop Services

Run:

    jps

You should normally see:

    NameNode
    DataNode
    ResourceManager
    NodeManager

## 13. Run MapReduce Job

Run:

    hadoop jar /home/ganesh/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
    -files mapper.py,reducer.py \
    -input /wordcount/input \
    -output /wordcount/output \
    -mapper mapper.py \
    -reducer reducer.py

## 14. Explanation of the Command

hadoop jar

Runs a Hadoop JAR.

hadoop-streaming-3.4.2.jar

Hadoop Streaming JAR that allows us to use Python for Mapper and Reducer.

-files mapper.py,reducer.py

Sends mapper.py and reducer.py to the MapReduce tasks.

-input /wordcount/input

Specifies the HDFS input directory.

-output /wordcount/output

Specifies the HDFS output directory.

-mapper mapper.py

Specifies the Mapper program.

-reducer reducer.py

Specifies the Reducer program.

## 15. MapReduce Process

HDFS Input:

    /wordcount/input/input.txt

↓

Mapper:

    Hadoop is good

↓

    hadoop    1
    is        1
    good      1

↓

Shuffle and Sort:

    big       [1,1,1]
    data      [1,1,1]
    good      [1,1]
    hadoop    [1,1,1,1]
    is        [1,1,1,1]
    powerful  [1,1]
    used      [1]

↓

Reducer:

    big       3
    data      3
    good      2
    hadoop    4
    is        4
    powerful  2
    used      1

↓

HDFS Output:

    /wordcount/output/

## 16. Check Output Directory

Run:

    hdfs dfs -ls /wordcount/output

You should see:

    /wordcount/output/_SUCCESS
    /wordcount/output/part-00000

## 17. Why part-00000?

part-00000 is the output file produced by a reducer.

If there is one reducer:

    Reducer 0
        ↓
    part-00000

If multiple reducers are used, you may get:

    part-00000
    part-00001
    part-00002

Each reducer can create its own output file.

## 18. View Final Result

Run:

    hdfs dfs -cat /wordcount/output/part-00000

Example:

    big       3
    data      3
    for       2
    good      2
    hadoop    4
    is        4
    powerful  2
    used      1

## 19. What Is _SUCCESS?

Hadoop creates:

    _SUCCESS

This is a marker indicating that the MapReduce job completed successfully.

It does not contain the word count.

The actual output is stored in:

    part-00000

## 20. Local File Structure

    /home/ganesh/wordcount/
        input.txt
        mapper.py
        reducer.py

## 21. HDFS File Structure

    /wordcount/
        input/
            input.txt
        output/
            _SUCCESS
            part-00000

## 22. Important HDFS Commands

Display HDFS root:

    hdfs dfs -ls /

Create directory:

    hdfs dfs -mkdir -p /wordcount/input

Upload file:

    hdfs dfs -put input.txt /wordcount/input/

List files:

    hdfs dfs -ls /wordcount/input

Read file:

    hdfs dfs -cat /wordcount/input/input.txt

Read MapReduce output:

    hdfs dfs -cat /wordcount/output/part-00000

Remove directory:

    hdfs dfs -rm -r /wordcount

## 23. Run MapReduce Again

If /wordcount/output already exists, Hadoop will give an error because MapReduce does not normally overwrite an existing output directory.

Remove the old output:

    hdfs dfs -rm -r /wordcount/output

Then run the MapReduce command again:

    hadoop jar /home/ganesh/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.2.jar \
    -files mapper.py,reducer.py \
    -input /wordcount/input \
    -output /wordcount/output \
    -mapper mapper.py \
    -reducer reducer.py

## 24. Key Concepts

HDFS:
Distributed storage system of Hadoop.

NameNode:
Manages HDFS metadata and filesystem information.

DataNode:
Stores the actual data blocks.

Mapper:
Processes input and generates key-value pairs.

Example:

    hadoop → 1

Shuffle:
Transfers mapper output and groups the same keys.

Sort:
Sorts/groups keys before sending them to the reducer.

Reducer:
Aggregates the values.

Example:

    hadoop → [1,1,1,1]

becomes:

    hadoop → 4

Hadoop Streaming:
Allows MapReduce programs to be written using Python instead of Java.

part-00000:
Output file produced by a reducer.

_SUCCESS:
Indicates that the MapReduce job completed successfully.

## 25. Most Important Flow

    INPUT
    ↓
    HDFS
    ↓
    MAPPER
    ↓
    KEY-VALUE PAIRS
    ↓
    SHUFFLE AND SORT
    ↓
    REDUCER
    ↓
    FINAL OUTPUT
    ↓
    HDFS

## 26. One-Line Summary

HDFS stores the data, Mapper converts the data into key-value pairs, Shuffle and Sort groups the same keys, Reducer aggregates the values, and the final result is stored back in HDFS.