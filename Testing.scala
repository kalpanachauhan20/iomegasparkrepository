// Databricks notebook source
val data = sc.textFile("wasbs://iomegacontainerdatakalpana@iomegastorageaccscala.blob.core.windows.net/fakefriends.csv")
val mdata = data.collect

// COMMAND ----------

// MAGIC %md # Learning ADB professionally

// COMMAND ----------

////code for notebook to take parameters

val defaultMoviesUrl = "https://iomegastorageaccscala.blob.core.windows.net/iomegadata2/movies.csv"
val defaultRatingsUrl = "adl://iomegadatalakestorekalpana.azuredatalakestore.net/data/ratings.csv"
 
val moviesUrl = dbutils.widgets.text("moviesUrl","")
val ratingsUrl = dbutils.widgets.text("ratingsUrl", "")
 
var inputMoviesUrl = dbutils.widgets.get("moviesUrl")
 
if(inputMoviesUrl == null) {
  inputMoviesUrl = defaultMoviesUrl
}
 
var inputRatingsUrl = dbutils.widgets.get("ratingsUrl")
 
if(inputRatingsUrl == null) {
  inputRatingsUrl = defaultRatingsUrl
}

// COMMAND ----------

package com.microsoft.analytics.utils

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object MovieUtils{
  import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
 
def loadMovieNames(fileName: String): Map[Int, String] = {
  if(fileName == null || fileName == "") {
    throw new Exception("Invalid File / Reference URL Specified!");
  }
 
  implicit val codec = Codec("UTF-8")
 
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
 
  val lines = Source.fromURL(fileName).getLines
 
  lines.drop(1)
 
  var movieNames: Map[Int, String] = Map()
 
  for(line <- lines) {
    val records = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
    val movieId = records(0).toInt
    val movieName = records(1)
 
    movieNames += (movieId -> movieName)
  }
 
  movieNames
}
}


// COMMAND ----------

import com.microsoft.analytics.utils._

val broadcastedMovies = sc.broadcast(() => { MovieUtils.loadMovieNames(inputMoviesUrl)})

// COMMAND ----------

 spark.conf.set("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.client.id", "8eab7633-a0eb-46da-8943-3ac104c2cff0")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.credential", "WWefHMLDiWLA903yHv16KovwdMKYteK8+VdwGvY+yhY=")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------

 spark.conf.set("spark.hadoop.dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.client.id", "8eab7633-a0eb-46da-8943-3ac104c2cff0")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.credential", "WWefHMLDiWLA903yHv16KovwdMKYteK8+VdwGvY+yhY=")
spark.conf.set("spark.hadoop.dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.access.token.provider.type", spark.conf.get("dfs.adls.oauth2.access.token.provider.type"))
spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.client.id", spark.conf.get("dfs.adls.oauth2.client.id"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.credential", spark.conf.get("dfs.adls.oauth2.credential"))

spark.sparkContext.hadoopConfiguration.set("dfs.adls.oauth2.refresh.url", spark.conf.get("dfs.adls.oauth2.refresh.url"))

val ratingsData = sc.textFile(inputRatingsUrl)
val originalData = ratingsData.mapPartitionsWithIndex((index, iterator) => {
if(index == 0) iterator.drop(1)

 else iterator
})
val mappedData = originalData.map(line => { val splitted = line.split(",")

(splitted(1).toInt, 1)
})
val reducedData = mappedData.reduceByKey((x, y) => (x + y))
val result = reducedData.sortBy(_._2).collect
val finalOutput = result.reverse.take(10)
val mappedFinalOuptut = finalOutput.map(record => (broadcastedMovies.value()(record._1), record._2))

// COMMAND ----------



// COMMAND ----------

