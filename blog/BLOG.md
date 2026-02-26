## Spark 4 by example: Declarative pipelines
![img_2.png](img_2.png)

### Introduction
In this series of blog posts we will walk though the new features and capabilities introduced to Spark 4 major and all
current minor versions showcasing each by example you can run by easily yourself.

This post focuses on of the major freshly features Declarative Pipeline.

### Overview
Spark Declarative Pipelines (SDP) have been introduced in Spark 4.1 version. SDP removes a need to organize 
a Direct Acyclic Graph of transformations by doing this for you. All is needed to be done is just a register components
that should work together. SDP figures out dependencies and  
The key components of the declarative pipelines are:
- `Flows` - the 

#### References:
- [Spark Declarative Pipelines Programming Guide](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
- [Introducing Apache Spark® 4.1](https://www.databricks.com/blog/introducing-apache-sparkr-41)
- https://docs.databricks.com/aws/en/ldp/concepts
- https://spark.apache.org/docs/latest/api/python/reference/pyspark.pipelines.html
- https://medium.com/@willgirten/a-first-look-at-declarative-pipelines-in-apache-spark-4-0-751144e33278
- https://issues.apache.org/jira/browse/SPARK-51727
- [SPARK-48117: Spark Materialized Views: Improve Query Performance and Data Management](https://issues.apache.org/jira/browse/SPARK-48117)
- https://www.youtube.com/watch?v=WNPYEZ7SMSM