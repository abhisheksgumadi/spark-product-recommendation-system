# spark-recommendation-system
A product recommendation system in Spark using just product purchase history without any ratings

This program written in PySpark recommends a maximum of 5 new products to a user based on products other users have purchased.

The command to run the program is 

./spark-submit --master local[*] RecommendationSystem.py -user_id 10 -file_path purchases.csv

where purchases.csv is the file that has the purchase history data.

Please take a look at the sample data uplaoded in the data directory.
