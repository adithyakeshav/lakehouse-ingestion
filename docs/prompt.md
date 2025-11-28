## Original Prompt / Vision

The following is the original prompt describing the vision and requirements for this repository. It is kept here verbatim for future reference.

---

In the folder "/Users/adithya.k/ak/lakehouse-ingestion", I want to setup a one stop solution for all the ingestion problems at hand. 

This is a repo (Primarily on Spark), which will have adapter patterns for Reader and Writers, and will have the core logic for medallion architecture of the Data lakehouse. Lakehouse might be of Iceberg or Datalake. Catalog can be Hive Metastore or Nessie or PG Lake. Since it covers a lot of things, I want to go on Phased approach, where in first we will build the abstrat classes or interfaces that will define core ingestion, then work upon specific implementation. 

Also, I want to avoid Schema inference in many places, so that we can have explicit schema, that is taken into all jobs from one central place. Let's build a design around it. 
Also, I want to maintain Data Quality checks that will help people build confidence around it.

Phases will be as folllows -
1. Problem statement doc -> Should contain detailed explanation of all the problems, clearly defined in correct categories
2. Tech Spec -> How we can solve these issues, what is the tech solution for each of these.
3. High level design
4. Low Level Design 
5. Start coding with basic interfaces and abstract classes


