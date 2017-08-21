var CronJob = require('cron').CronJob;
var nrc = require('node-run-cmd');
var fs = require('fs');


var dir = '/home/devansh/Documents/GeoNodeService/';
  var cmd = '/usr/bin/pg_dump --host localhost --port 5432 --username "postgres" --no-password  --format tar --blobs --inserts --column-inserts --verbose --file "'+ getFileName() +'" "postgis"'

  var d = new Date();
  d.setDate(d.getDate()-10);
  var curdate= d.toJSON().slice(0, 10).replace(/-/g, '').toString();
  var listofFiles = fs.readdirSync(dir + 'dbBackup/')
  for(var i = 0; i < listofFiles.length;i++){
    var split = listofFiles[i].toString().split(/_|\./)
    if(curdate > split[1]){
      console.log(true);
      fs.unlink(dir + 'dbBackup/' + listofFiles[i], function(err){
    if(err)
      console.log(err);
      })
    }
  }

  console.log(cmd);
  

  var dataCallback = function(data) {
    console.log(data);
  };
  nrc.run(cmd, { onDone: dataCallback });





var job = new CronJob('00 00 04 */3 * *', function () {
  /*
   * Runs every 3 days at 4:00 AM
   */
  console.log(new Date().toString() + ": Running Job")
  // var dir = '/home/devansh/Documents/GeoNodeService/';
  // var cmd = '/usr/bin/pg_dump --host localhost --port 5432 --username "postgres" --no-password  --format tar --blobs --inserts --column-inserts --verbose --file "' + dir + getFileName() + '.backup" "postgis"'

  // var d = new Date();
  // d.setDate(d.getDate()-10);
  // var curdate= d.toJSON().slice(0, 10).replace(/-/g, '').toString();
  // var listofFiles = fs.readdirSync(dir + 'dbBackup/')
  // for(var i = 0; i < listofFiles.length;i++){
  //   var split = listofFiles[i].toString().split(/_|\./)
  //   if(curdate > split[1]){
  //     console.log(true);
  //     fs.unlink(dir + 'dbBackup/' + listofFiles[i], function(err){
  //   if(err)
  //     console.log(err);
  //     })
  //   }
  // }
  

  // var dataCallback = function(data) {
  //   console.log(data);
  // };
  // nrc.run(cmd, { onData: dataCallback });



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
  fs.appendFile(file, error + '\n', function (err) {
    if (err) {
      console.log(err);
    }
  });
}
function getFileName(){
    return '/home/devansh/Documents/GeoNodeService/dbBackup/backup_'+ new Date().toJSON().slice(0, 10).replace(/-/g, '') + '.backup'
  }
