# ETL

Il primo applicativo è un ETL che provveda ad interrogare le API delle sorgenti e inserisca i valori in un database.
La scelta è così assortita:

[Apache Airflow ](https://airflow.apache.org/)

Apache Airflow® is a platform created by the community to programmatically author, schedule and monitor workflows.

[Apache Hop](https://hop.apache.org/)

An open source platform for data integration and orchestration. 

### Confronto

[Apache Airflow vs Apache Hop: Which Tool is Better for Your Next Project?](https://www.projectpro.io/compare/apache-airflow-vs-apache-hop)

### Alternative

[Luigi ](https://github.com/spotify/luigi)

Luigi is a Python (3.10, 3.11, 3.12, 3.13, 3.14 tested) package that helps you build complex pipelines of batch jobs. 

[Azkaban](https://azkaban.readthedocs.io/en/latest/index.html)

Azkaban is a distributed Workflow Manager, implemented at LinkedIn to solve the problem of Hadoop job dependencies. We had jobs that needed to run in order, from ETL jobs to data analytics products.

