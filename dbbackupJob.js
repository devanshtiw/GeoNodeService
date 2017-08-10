var CronJob = require('cron').CronJob;
var pg = require('pg');
var fs = require('fs');

var job = new CronJob('00 00 04 * * *', function() {
  /*
   * Runs every 3 days at 4:00 AM
   */
   console.log(new Date().toString() + ": Running Job");
   

   console.log(new Date().toString() + ": Job Finished");
  }, function () {
	console.log("Done");
    /* This function is executed when the job stops */
  },
  true, /* Start the job right now */
  'Asia/Kolkata' /* Time zone of this job. */
);

function logError(error) {
    var file = 'logs/dbbackupJob' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
    fs.appendFile(file, error + '\n', function(err) {
        if (err) {
            console.log(err);
        }
    });
}
