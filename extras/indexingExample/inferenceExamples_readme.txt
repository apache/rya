
USAGE FOR InferenceExamples.java

----------------
Notes on MongoDB Setup:

If you are using single-instance mongodb mode.

If the first time you have installed MongoDB you will need to create a rya database.
You will also need to create a username and password for the rya database.
After logging into the MongoDB database you can use the commands below to accomplish the above.

use rya
db.createUser({user:"urya",pwd:"urya",roles:[{role:"readWrite",db:"rya"}]})

----------------

Notes on how to use embedded and single-instance.

To use embedded mode set the USE_EMBEDDED_MONGO to true.

To use single-instance set the USE_EMBEDDED_MONGO to false.
Also setup the MongoUserName and MongoUserPassword.


----------------

Notes on error for single-instance.

If you have run the InerenceExample more than once in single-instance mode, you may get an error.
This is because the tables have been created in MongoDB.
Run the below to remove the tables via client mongo login. 
After running a table list you will see no tables: show tables
You then may run the examples again.

use rya
show tables
db.rya__triples.drop()
db.rya_rya_freetext.drop()
db.rya_rya_temporal.drop()
exit


----------------
