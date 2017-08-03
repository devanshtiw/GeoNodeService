var express = require('express'),
    bodyParser = require('body-parser'), //Middle ware for parsing data like JSON data
    methodOverride = require('method-override'),
    cookieParser = require('cookie-parser'),
    pg = require('pg'),
    compress = require('compression'),
    router = express.Router(), //using express route
    port = process.env.PORT || 9000; //setting port
app = module.exports = express();
http = require('http').Server(app);

const connectionString = process.env.DATABASE_URL || "postgres://postgres:postgres@localhost:5432/geoserver";
const client = new pg.Client(connectionString);
client.connect(function(err) {
    if (err)
        logError("server: Connection Failed. \n" + err);
});

client.on('error', function(err) {
	console.log('server: Connection Terminated!');
});

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

router.all("/api/*", function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Authentication, Content-Type, X-Requested-With");
    res.header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS, HEAD");
    res.header('Content-Type', 'application/json');
    //res.header('Content-Type', 'text/plain');
    return next();
});

var wcsDataService = require('./routes/wcsDataService')(client);
var layerDataService = require('./routes/layerDataService')(client);

router.post('/api/wcs', wcsDataService.updateWcsBusinessData)

router.post('/api/layerservice', layerDataService.getAssociationData)

 router.get('/api/nearestStructures', wcsDataService.getNearestWCSStructures)

var server = http.listen(port, function() {
    var host = server.address().address
    var port = server.address().port
    console.log("App listening at http://%s:%s", host, port)
});
