# LargeScaleParallelProcessingProject

This Project's objective is to forecast future weather temperatures for all states by choosing the best model among ARIMA and LSTM. For forecasting, used the Spark is used as base platform running in AWS (Amazon Web Services) GLUE for ML modeling. 


As for data analysis, Pig, Hive and Hcatalog were used and entire data engineering pipeline was created on AWS EMR:- 
  1) PIG Latin scripts for data preprocessing
  2) Hcatalog to share metadata between Pig and Hive tables
  3) Hive to create tables using Hadoop EMR
  4) HUE and Quick Sight to generate plots, view the data and gain insights.
