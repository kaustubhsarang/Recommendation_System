import re;
import os;
import sys;
from pyspark import SparkConf, SparkContext, HiveContext;
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating;

# Initialization of spark context
sc = SparkContext(master="local",appName="Spark Demo");
sqlContext = HiveContext(sc);
# parameters for AWS
args=sys.argv;
inp1=args[1];
inp2=args[2];
out=args[3];

# Ratings.csv is taken as input
ratings = sc.textFile(inp1);
# ratings = sc.textFile("G:\My Drive\UIC SEM 2\Big data\Assignment\project\\ratings_int.csv",4000);
data_header = ratings.take(1)[0];
ratings = ratings.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
# Convert csv in required format
#
# convert csv in required format end
# business = sc.textFile("G:\My Drive\UIC SEM 2\Big data\Assignment\project\\business_int.csv",4000);
# business.csv is taken as input
business = sc.textFile(inp2);
data_header = business.take(1)[0];
business = business.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1])).cache()

new_user_ID = 3

# new user is created and we predict ratings for these records.
new_user_ratings = [
     (3,10,4),
     (3,14,3),
     (3,19,3),
     (3,22,4),
     (3,32,4),
     (3,44,1),
     (3,53,1),
     (3,63,3),
     (3,70,5),
     (3,104,4)
    ]
new_user_ratings_RDD = sc.parallelize(new_user_ratings)
ratings_new_user = ratings.union(new_user_ratings_RDD);

rank=10;
numIterations=10;

# model is created
model = ALS.train(ratings_new_user, rank,numIterations);

#get business id from new user
new_user_ratings_ids = map(lambda x: x[1], new_user_ratings)
new_user_unrated_RDD = (business.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))
new_user_recommendations_RDD = model.predictAll(new_user_unrated_RDD);
new_user_recommendations_RDD=new_user_recommendations_RDD.map(lambda x:(x.product,x.rating));
result=new_user_recommendations_RDD.join(business);
result=result.map(lambda a:(a[1][1],a[1][0]));
result=result.map(lambda a:[a[0],a[1]])


def tocsv(data):
    return  ','.join(str(d) for d in data);


lines = result.map(tocsv);

lines=lines.map(lambda a:re.sub('\[','',a));
lines=lines.map(lambda a:re.sub(']','',a));

lines.saveAsTextFile(out);
# new_user_recommendations_RDD.saveAsTextFile(out);
# new_user_recommendations_RDD.saveAsTextFile("project_predict");

sc.stop();


