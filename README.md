
# HashtagSim

## I. Background 

This program analyzes the similarities between hashtags, which are used primarily in Twitter.com to label the tweets. A hashtag is denoted by a ‘#’ followed by a word. For example, a recent tweet from Barack Obama reads: “The excuses not to #ActOnClimate need to end”. In this tweet, “#ActOnClimate” is a hashtag.

Hashtags are used to categorize tweets, promote events, etc. In our program, we try to identify how similar the hashtags are. We’re using the words that co­occurred with a hashtag as its features. For example, given a tweet “#a b c”, word “b” and “c” will have both co­occurred with hashtag “#a” in the tweet for once. Given the following corpus:

```
#a b c#a b #b 
#b #c d e 
#c e f
```

The co­occurrence counts for each hashtag, or put in another way, the features for the hashtags, will look like the following:

```
#a b:2;c:1;#b b:1;d:1;e:1; 
#c d:1;e:2;f:1;
```

Note that here we are treating hashtag “#b” and word “b” differently. Also, we’re using both the co­occurred words and the co­occurrence counts to represent a hashtag.

After calculating the feature vector for the hashtags, we can compute the similarity between hashtag pairs, using inner product of the feature vectors. The inner product of two feature vectors is calculated by picking all the common words and summing up the products of the co­occurrence value.

Taken the above result as example, the inner product between “#a” and “#b” will be 2, since they have only 1 shared word, “b”, and the product is 2\*1. The inner product between “#b” and “#c” will be 3, which is result of 1\*1 (d) + 1*2 (e). The bigger the inner product is, the more similar the two hashtags are in the given corpus.

## II. Performance
**Run-Time**: ***22*** mins(The original version needs 90 mins)

**Cluster**: EMR cluster of 5 medium machines(c1.medium, 1 master + 4 slaves).

**MapReduce**: MapReduce Framework 1 by using ami­-version 2.4.11.

(PS: If you use **MapReduce 2** which is built on a system called **YARN**(Yet Another Resource Negotiator), you can get better performance.)  

## III. How to Run?
### Step 1. Download
```
# Download repo
$ git clone https://github.com/linquanchen/HashtagSim.git

# Download data
$ wget https://s3.amazonaws.com/jeffery/hashtagsim/tweets1m/tweets1m.txt
```
### Step 2. Install prerequisites
```
# install JDK 1.7
$ sudo apt­-get install openjdk­-7-­jdk

# install ant
$ sudo apt-get install ant

# Set JAVA_HOME environment variable$ export JAVA_HOME=/usr/lib/jvm/java­-7-­openjdk-­amd64 
$ export PATH=$JAVA_HOME/bin:$PATH
```
### Step 3. Compile
```
$ cd HashtagSim
$ ant
# On a successful build, it will show “BUILD SUCCESSFUL”. 
# Under HashtagSim, there will be a new file - 18645_project3_team004.jar.
```
### Step 4. Upload files to Amazon S3
```
# cd HashtagSim

# change <bucketname to your own bucket name>
# create bucket for jar file and upload
$ aws s3 mb s3://<bucketname>
$ aws s3 cp 18645_project3_team004.jar s3://<bucketname>

# create bucket for output and upload data
$ aws s3 mb s3://<bucketname>.tweets1m$ aws s3 cp /data/tweets1m/tweets1m.txt

# create bucket for output
$ aws s3 mb s3://<bucketname>.output 

# create bucket for job logs$ aws s3 mb s3://<bucketname>.log-­uri.hashtagsim
```
### Step 5. Launch the cluster and run the job
```
aws emr create­-cluster --­­name "hashtagsim" --ami-­version 2.4.11 --service­-role EMR_DefaultRole --ec2-­attributes InstanceProfile=EMR_EC2_DefaultRole --log-­uri  s3://<bucketname>.log-uri.hashtagsim --enable­-debugging --instance­-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=4,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Jar=s3://<bucketname>/18645_project3_team004.jar,Args=["-­input","s3://<bucketname>.tweets1m/tweets1m.txt","­-output","s3://<bucketname>.output/hashtag1m","-program","hashtagsim","­-tmpdir","tmp"] --­­auto­-terminate
```

### Step 6. Download output file
```
# You can download the output file from S3:
s3://<bucketname>.output/hashtag1m/*
```