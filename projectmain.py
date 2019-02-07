


from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
spark = SparkSession \
     .builder \
     .appName(" movie data") \
     .getOrCreate()

data = spark.read\
    .format("csv")\
    .option("Header","true")\
    .load("D:\imdb.csv")

data.dropna()

rating_data = data.select('title','imdbRating')\
            .distinct()
title1 = [x[0] for x in rating_data.toLocalIterator()]
title = title1[0]

def moviereview(name):
    movie_details = rating_data.filter(rating_data.title == name)
    rating =  [x[1] for x in movie_details.toLocalIterator()]
    imdb_rating = float(rating[0])
    rating_per = imdb_rating*10
    rating_agr = 100 - rating_per
    print(imdb_rating)
    print(rating_per)
    print(rating_agr)
    labels = 'Positive','Negative'
    sizes = [rating_per,rating_agr]
    explode = (0.2,0)  # only "explode" the 2nd slice (i.e. 'Hogs')

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
        shadow=True, startangle=0)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.show()

moviereview(title)