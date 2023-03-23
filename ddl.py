# Databricks notebook source
def createDDL(database):
    alltables = spark.catalog.listTables(database)
    f=open("/tmp/ddls/bkp_{}.sql".format(database),"w")
    for t in alltables:
        ddls = spark.sql("SHOW CREATE TABLE {}.{};".format(database,t.name))
        f.write(ddls.first()[0])
        f.write(";\n")
    f.close()
%fs ls file:/tmp/ddls/