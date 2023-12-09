List of tasks to do:

Batch layer:

    [ ] combine json data
    [ ] convert them into parquet - pySpark
    [ ] save data as one table to Cloud Storage
    
    [ ] create and fit clusterring model
    [ ] create rule for refitting model

Speed layer

    [ ] load actual model from batch layer   
    [ ] combine data from stream in the same way as before
    [ ] create process for labeling new data from stream
    [ ] save data in the serving layer

Serving layer 

    [ ] create serving layer with Cassandra
    [ ] find solution to download data from batch layer
    [ ] find solution to catch data from speed layer
    [ ] prepare outcome for the consumption layer

Consumption layer

    [ ] create dashboard as a python streamlit file
    [ ] host dashboard locally/online
    [ ] prepare methods for connecting to the data from serving layer
    [ ] create UI