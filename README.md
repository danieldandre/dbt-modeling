# dbt-modeling

Example of DBT (Data Build Tool) models I have developed and deployed into a production pipeline. 

## Social Session Models

In a given platform we have social instances. 
Users create these social instances, typically when sharing the result of a given game to their own feed or when inviting a friend directly to compete against their scores or to participate/play in the play session they're in. 

The biggest challenge with building this social session model is that we do not have an event that identifies when a session is created. The telemetry is janky with current event semantics and definition not allowing us to pinpoint when a user becomes part of a social session. 

The reason we need this model is because without it we cannot understand or interpret the values that social features bring to our games. Social features are an added bonus of working within a certain platform - we expect users to share the games among each other, and the prompt for users to jump back in those games contributes positively towards metrics such as retention. 



## Play Session Properties

The whole premise behind this table is we have staging layers that we need to transform or to otherwise apply some business logic. From this model we can derive applications to build or enrich a bunch of other models. 

For example: 

- the main use of this table is for us to enrich a "streaming" model at some point in our DAG. This business logic usually includes some window functions (same ones that can be seen) that are not ideal when dealing with event-level granularity and scanning large amounts of data. 
- another important feature used across multiple models is what we have taken to call "user_properties" - in essence a table that gives us data regarding when a user was acquired. Obviously this could be done by scanning directly from a staging layer, but as those layers grow more and more each day, we want to avoid that. Furthermore, those require window functions that are very costly in terms of processing resources and can lead to timeouts. 
- somewhere downstream we'll need to create a table that calculates user retention. For each of those cohorts, we want to associate the first values of certain attributes when the user was acquired. 

For the examples above, and given how central play_session_properties is within our DAG, how many tables and how many clients it serves and how it works around an important technical constraint (processing time) I have chosen it as an example. 

