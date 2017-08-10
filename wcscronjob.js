var CronJob = require('cron').CronJob;
var pg = require('pg');
var fs = require('fs');

var job = new CronJob('00 00 04 * * *', function() {
  /*
   * Runs everyday at 04:00:00 AM
   */
   console.log(new Date().toString() + ": Running Job");
   const connectionString = process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/geoserver";
   const client = new pg.Client(connectionString);

   client.on('error', function (err) {
     console.log('Connection Terminated!');
   });

   client.connect(function(err){
     if(err)
       logError("Connection Failed. \n" + err);
   });

   var querystmt = 'UPDATE "iwm_data" SET iwm_timest = null, iwm_image_ = null, iwm_storag = null  where ' +
   'DATE_PART(\'day\', current_date::timestamp - to_date(iwm_timest,\'YYYYMMDD\')::timestamp) > 30;';
   query = client.query(querystmt, function(err, result) {
       if (err) {
           logError('Unable to run the query \n' + querystmt);
           logError(err);
           console.log(err);
       } else {
           console.log(result.rowCount + 'row Updated');
       }
   });
   console.log(new Date().toString() + ": Job Finished");
  }, function () {
	console.log("Done");
    /* This function is executed when the job stops */
  },
  true, /* Start the job right now */
  'Asia/Kolkata' /* Time zone of this job. */
);

function logError(error) {
    var file = 'logs/cronJob' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
    fs.appendFile(file, error + '\n', function(err) {
        if (err) {
            console.log(err);
        }
    });
}
