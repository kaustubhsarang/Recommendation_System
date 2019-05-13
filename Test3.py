import re;
import os;
import sys;
from pyspark import SparkConf, SparkContext, HiveContext;
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating;


sc = SparkContext(master="local",appName="Spark Demo");
sqlContext = HiveContext(sc);


ratings = sc.textFile("G:\My Drive\UIC SEM 2\Big data\Assignment\project\\ratings_int_new.csv",2000);
data_header = ratings.take(1)[0];
ratings = ratings.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()

business = sc.textFile("G:\My Drive\UIC SEM 2\Big data\Assignment\project\\business_int_new.csv",2000);
data_header = business.take(1)[0];
business = business.filter(lambda line: line!=data_header).map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1])).cache()

new_user_ID = 3

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

model = ALS.train(ratings_new_user, rank,numIterations);


new_user_ratings_ids = map(lambda x: x[1], new_user_ratings)
new_user_unrated_RDD = (business.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))
new_user_recommendations_RDD = model.predictAll(new_user_unrated_RDD)
# testdata = ratings.map(lambda p: (p[0], p[1]));
# predictions = model.predictAll(testdata).map(lambda r: (r[0], r[1], r[2]));

# new_user_recommendations_RDD.saveAsTextFile("project_predict");
new_user_recommendations_RDD=new_user_recommendations_RDD.map(lambda x:(x.product,x.rating));
result=new_user_recommendations_RDD.join(business);
result=result.map(lambda a:(a[0],a[1][0],a[1][1]));
result.saveAsTextFile("joinedresult");

sc.stop();


