var express = require('express'),
    bodyParser = require('body-parser'), //Middle ware for parsing data like JSON data
    methodOverride = require('method-override'),
    cookieParser = require('cookie-parser'),
    compress = require('compression'),
    jsonfile = require('jsonfile'),
    pg = require('pg'),
    fs = require('fs'),
    router = express.Router(), //using express route
    port = process.env.PORT || 9000; //setting port
app = module.exports = express();
http = require('http').Server(app);



app.use(compress());
app.use(bodyParser.json({
    limit: '50mb'
})); // parse application/json pull information from html in POST
app.use(bodyParser.urlencoded({
    parameterLimit: 100000,
    limit: '50mb',
    extended: true
}));
app.use(bodyParser.raw({
    type: 'application/vnd.custom-type'
}));
app.use(bodyParser.text());
app.use(methodOverride()); // simulate DELETE and PUT
app.use(cookieParser());
app.use('/', router);

const connectionString = process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/geoserver";
const client = new pg.Client(connectionString);
client.connect(function(err){
  if(err)
    logError("Connection Failed. \n" + err);
});

client.on('error', function (err) {
  console.log('Connection Terminated!');
});



router.all("/api/*", function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Authentication, Content-Type, X-Requested-With");
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS, HEAD");
    res.header('Content-Type', 'application/json');
    //res.header('Content-Type', 'text/plain');
    return next();
});


router.post('/api/wcs', function(req, res) {
    // console.log(req);
    // The form's action is '/' and its method is 'POST',
    // so the `app.post('/', ...` route will receive the
    // result of our form
    var data = req.body;
	// var data = jsonfile.readFileSync("D:\\node\\logs\\JSON_1501011538926.json");
    //If no data is received, it will send back response 0
    if (Object.keys(data).length == 0)
        return res.send("0");
    else
        console.log(new Date().toString() + ": Data Received");
    var file = 'logs/JSON_' + new Date().getTime().toString() + '.json';
    jsonfile.writeFile(file, data, function(err) {
        if (err)
            console.log(err);
    });
    res.send("201");
    var keys = dateFormatter(data);
    var rcount = 0;
    for (var i = 0; i < keys.length; i++) {
        if (data.hasOwnProperty(keys[i])) {
            for (var id in data[keys[i]]) {
                if (data[keys[i]].hasOwnProperty(id)) {
                    var rowd = getFormattedRowData(data[keys[i]][id]);
                    if (rowd != -1) {
                        insert(id, rowd);
                        rcount++;
                    } else {
                        logError('ID ' + id + ' does not have property dailyStorageData OR Data is more than 21 days old OR Timestamp is invalid. \n');
                    }
                }
            }
        }
    }
    console.log(new Date().toString() + ": Rows Updated: " + rcount);
});

var server = http.listen(port, function() {
    var host = server.address().address
    var port = server.address().port

    console.log("App listening at http://%s:%s", host, port)
});

function getFormattedRowData(data) {
    if (data.hasOwnProperty('dailyStorageData')) {
        var row = [];
        var latestData = data.dailyStorageData.toString().split(',');
        var arr1 = latestData[latestData.length - 1].toString().split('=');
        var date = parseDate(arr1[0]);
        var curDate = new Date();
        var timeDiff = Math.abs(curDate.getTime() - date.getTime());
        var diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));
        if (diffDays > 30 || date == "invalid date")
            return -1;
        if (arr1.length >= 1)
            row.push(arr1[0]);
        else
            row.push(null);
        var arr2 = arr1[1].toString().split('#');
        if (arr2.length >= 1)
            row.push(arr2[0]);
        else
            row.push(null);
        if (data.hasOwnProperty('imageURL')) {
            row.push(data.imageURL);
        } else {
            row.push(null);
        }
        if (arr2.length == 2)
            row.push(arr2[1]);
        else
            row.push(null);
        return row;
    } else {
        return -1;
    }
}

function dateFormatter(data) {
    var dates = [];
    for (var date in data)
        dates.push(date);
    dates.sort();
    return dates;
}

//Function to insert rows in the tables with the data received in the API.
function insert(id, rowdata) {

    var querystmt = prepareQuery("iwm_data", rowdata, id);
    query = client.query(querystmt, function(err, result, rval) {
        if (err) {
            logError('Unable to run the query \n' + querystmt);
            logError(err);
            console.log(err);
        } else {
            // console.log(result.rowCount + 'row Updated');
            if (result.rowCount == 0)
                logError("Not Changed for: " + id + "\n");
        }
    });

}

function prepareQuery(tablename, rowdata, id) {
    var query = 'UPDATE "' + tablename + '" SET iwm_timest =\'' + rowdata[0] + '\' , iwm_storag = \'' +
        rowdata[1] + '\' , iwm_image_ = \'' + rowdata[2] + '\' , source_type = \'' + rowdata[3] + '\' WHERE iwm_wcs_id = \'' + id + '\'';
    return query;
}

//Function to lof error in the file format (Log_Date.txt) and the error text passed as parameter to the function.
function logError(error) {
    var file = 'logs/Log' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
    fs.appendFile(file, error + '\n', function(err) {
        if (err) {
            console.log(err);
        }
        // else
        // console.log("The Error file was saved!");
    });
}

function parseDate(str) {
    var y = str.substr(0, 4),
        m = str.substr(4, 2) - 1,
        d = str.substr(6, 2);
    var D = new Date(y, m, d);
    return (D.getFullYear() == y && D.getMonth() == m && D.getDate() == d) ? D : 'invalid date';
}