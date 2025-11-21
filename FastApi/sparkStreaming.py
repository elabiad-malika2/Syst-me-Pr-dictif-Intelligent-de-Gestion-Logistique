import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, isnan, isnull, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)
import psycopg2
from pymongo import MongoClient
from pyspark.sql import functions as F


import shutil
checkpoint_dir = "c:/Users/elabi/OneDrive/Desktop/Gestion Logistique/FastApi/checkpoint"
shutil.rmtree(checkpoint_dir, ignore_errors=True)

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

def start_streaming(spark):
    # === SCHEMA EXACT SELON api.py (11 champs avec espaces) ===
    schema = StructType([
        StructField("Days for shipment (scheduled)", IntegerType(), True),
        StructField("Order Region", StringType(), True),
        StructField("Sales", DoubleType(), True),
        StructField("Order Item Quantity", IntegerType(), True),
        StructField("Order Item Profit Ratio", DoubleType(), True),
        StructField("Order City", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Customer Segment", StringType(), True),
        StructField("Category Name", StringType(), True),
        StructField("Sales per customer", DoubleType(), True),
        StructField("event_time", StringType(), True)
    ])

    # === Flux TCP ===
    raw_df = (
        spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()
    )

    # === JSON -> colonnes ===
    parsed = (
        raw_df
        .select(
            from_json(col("value"), schema).alias("data")
        )
        .select("data.*")
        .filter(col("data").isNotNull())
    )

    # Convertir event_time en timestamp
    parsed = parsed.withColumn(
        "event_time",
        col("event_time").cast(TimestampType())
    )

    # Remplacer NaN pour les colonnes numériques
    numeric_cols = [c for c in parsed.columns 
                    if c != "event_time" and 
                    parsed.schema[c].dataType in [DoubleType(), IntegerType()]]
    
    select_exprs = []
    for c in parsed.columns:
        if c in numeric_cols:
            select_exprs.append(
                when(isnan(col(c)) | isnull(col(c)), None).otherwise(col(c)).alias(c)
            )
        else:
            select_exprs.append(
                when(isnull(col(c)), None).otherwise(col(c)).alias(c)
            )
    
    parsed = parsed.select(select_exprs)

    # Filtrer les lignes avec des données nulles critiques
    parsed = parsed.filter(
        col("Days for shipment (scheduled)").isNotNull() &
        col("Order Region").isNotNull() &
        col("Sales").isNotNull()
    )

    # === CHARGER LE MODÈLE ===
    from pyspark.ml import PipelineModel
    
    MODEL_PATH = r"C:\Users\elabi\OneDrive\Desktop\Gestion Logistique\notebooks\workspace\models\mymodel"
    
    if not os.path.exists(MODEL_PATH):
        print(f"ERREUR: Modèle introuvable: {MODEL_PATH}")
        return
    
    try:
        pipeline_model = PipelineModel.load(MODEL_PATH)
        print(f"Modèle chargé: {MODEL_PATH}")
    except Exception as e:
        print(f"ERREUR chargement: {e}")
        import traceback
        traceback.print_exc()
        return

    # Enlever event_time avant le modèle (s'il n'était pas dans l'entraînement)
    parsed_for_model = parsed.drop("event_time")
    
    # DEBUG: Afficher les colonnes avant le modèle
    print(f"Colonnes avant modèle: {parsed_for_model.columns}")
    
    try:
        predictions_df = pipeline_model.transform(parsed_for_model)
        print("Pipeline appliqué avec succès")
        print(f"Colonnes après pipeline: {predictions_df.columns}")
    except Exception as e:
        print(f"ERREUR lors de l'application du pipeline: {e}")
        print("\nColonnes disponibles dans parsed_for_model:")
        print(parsed_for_model.columns)
        print("\nAperçu des données:")
        parsed_for_model.show(3, truncate=False)
        import traceback
        traceback.print_exc()
        return

    if "prediction" not in predictions_df.columns:
        print("ERREUR: Colonne 'prediction' manquante")
        print(f"Colonnes disponibles: {predictions_df.columns}")
        return

    # === POSTGRESQL ===
    postgres_options = {
        "url": "jdbc:postgresql://localhost:5432/logistics_db",
        "driver": "org.postgresql.Driver",
        "dbtable": "stream_predictions",
        "user": "postgres",
        "password": "malika123"
    }

    def save_to_postgres(batch_df, batch_id):
        try:
            count = batch_df.count()
            print(f"Batch {batch_id}: {count} lignes")
            
            if count > 0:
                # Afficher un aperçu pour vérifier
                print("Aperçu des données:")
                batch_df.select(
                    "Days for shipment (scheduled)",
                    "Order Region",
                    "Sales",
                    "prediction"
                ).show(3, truncate=False)
                
                # Ajouter event_time
                result_df = batch_df.withColumn("event_time", current_timestamp())
                
                # Sélectionner toutes les colonnes
                all_cols = [c for c in result_df.columns]
                save_cols = [
                        "Days for shipment (scheduled)",
                        "Order Region",
                        "Sales",
                        "Order Item Quantity",
                        "Order Item Profit Ratio",
                        "Order City",
                        "Latitude",
                        "Longitude",
                        "Customer Segment",
                        "Category Name",
                        "Sales per customer",
                        "prediction",
                        "event_time"  # you add this
                    ]
                (
                    result_df
                    .select(*save_cols)
                    .write
                    .mode("append")
                    .format("jdbc")
                    .options(**postgres_options)
                    .save()
                )
                print(f"✓ Batch {batch_id} sauvegardé ({count} lignes)")
                # === AGGREGATION POUR INSIGHTS ===
                aggregations_df = batch_df.groupBy("Order Region").agg(
                    F.avg("Sales").alias("avg_sales"),
                    F.sum("Sales").alias("total_sales"),
                    F.count("*").alias("count_records"),
                    F.avg("prediction").alias("avg_prediction")
                )

                # Convertir en dictionnaires Python
                agg_results = [row.asDict() for row in aggregations_df.collect()]

                # === SAUVEGARDE DANS MONGODB ===

                try:
                    mongo = MongoClient("mongodb://localhost:27017/")
                    mongo_db = mongo["logistics_insights"]
                    mongo_collection = mongo_db["streaming_aggregations"]

                    # Insérer les résultats
                    if len(agg_results) > 0:
                        mongo_collection.insert_many(agg_results)
                        print(f"✓ Agrégations stockées dans MongoDB : {len(agg_results)} documents")
                    else:
                        print("✗ Aucune agrégation à insérer dans MongoDB")

                except Exception as mongo_error:
                    print(f"ERREUR MongoDB: {mongo_error}")

            else:
                print(f"✗ Batch {batch_id} vide")
        except Exception as e:
            print(f"ERREUR batch {batch_id}: {e}")
            import traceback
            traceback.print_exc()

    query = (
        predictions_df.writeStream
        .foreachBatch(save_to_postgres)
        .outputMode("append")
        .trigger(processingTime='2 seconds')
        .option("checkpointLocation", os.path.join(PROJECT_DIR, "checkpoint"))
        .start()
    )

    print("Streaming démarré. Ctrl+C pour arrêter.")
    query.awaitTermination()

if __name__ == "__main__":
    try:
        spark = (
            SparkSession.builder
            .appName("SmartLogisticsStream")
            .master("local[*]")
            .config("spark.jars", r"C:\Users\elabi\Downloads\postgresql-42.7.3.jar")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")
        print("Spark Session créée")
        start_streaming(spark)
    except Exception as e:
        print(f"ERREUR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()
            print("Spark Session arrêtée")