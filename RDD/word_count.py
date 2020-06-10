#!pip install pyspark
from pyspark import SparkConf
from pyspark import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

# load movies file
movie_file = 'C:\\Users\\prshn\\Downloads\\ml-latest-small\\movies.csv'
movie_rdd = sc.textFile(movie_file)
movie_rdd.take(1)
movie_rdd_wh = movie_rdd.subtract(sc.parallelize(movie_rdd.first()))
movieid_genre = movie_rdd_wh.map(lambda line: (line.split(',')[0], line.split(',')[2]))


# load rtings file
ratings_file = 'C:\\Users\\prshn\\Downloads\\ml-latest-small\\ratings.csv'
ratings_rdd = sc.textFile(ratings_file)
ratings_rdd_wh = ratings_rdd.subtract(sc.parallelize([ratings_rdd.first()]))
movieid_ratings = ratings_rdd_wh.map(lambda line: (line.split(',')[1], line.split(',')[2]))


# join the movies and ratings rdd
# filter thriller movies
joined = movieid_genre.join(movieid_ratings)
thriller = joined.filter(lambda line: 'Thriller' in line[1][0])
#thriller.take(4)

# swap tuple values
thriller_swap = thriller.map(lambda elem: {float(elem[1][1]), (elem[1][0], elem[0])})
# thriller_swap.take(4)


# sort by ratings
from operator import itemgetter
thriller_sorted = thriller_swap.sortBy(lambda x: x[0])
# thriller_sorted.take(5)