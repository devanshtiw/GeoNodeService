'use strict';

var jsonfile = require('jsonfile'),
    fs = require('fs');


module.exports = function (client) {

    var service = {
        updateWcsBusinessData: updateWcsBusinessData,
        getNearestWCSStructures: getNearestWCSStructures
    }

    function updateWcsBusinessData(req, res) {
        // console.log('wcsDataService: ' + req);
        // The form's action is '/' and its method is 'POST',
        // so the `app.post('/', ...` route will receive the
        // result of our form
        var data = req.body;
        // var data = jsonfile.readFileSync("JSON_wcsService1502058101483.json");
        //If no data is received, it will send back response 0
        if(req.headers['content-type'] != 'application/json')
            return res.json('Content not application/json')
        if (Object.keys(data).length == 0)
            return res.send("0");
        else
            console.log('wcsDataService: ' + new Date().toString() + ': Data Received');
        var file = 'logs/JSON_wcsService' + new Date().getTime().toString() + '.json';
        jsonfile.writeFile(file, data, function (err) {
            if (err)
                console.log('wcsDataService: ' + err);
        });
        res.send("201");

        var keys = ((data) => {
            var dates = [];
            for (var date in data)
                dates.push(date);
            dates.sort();
            return dates;
        })(data);
        // console.log(keys.length);
        var rcount = 0;
        for (var i = 0; i < keys.length; i++) {
            for (var id in data[keys[i]]) {
                var rowd = getFormattedRowData(data[keys[i]][id]);
                if (rowd != -1) {
                    insertWCSData(id, rowd);
                    rcount++;
                } else {
                    logError('wcsDataService: ID ' + id + ' does not have property dailyStorageData OR Date is more than 30 days old. \n');
                }
            }
        }
        console.log('wcsDataService: ' + new Date().toString() + ": Rows Processed: " + rcount);
    }

    async function getNearestWCSStructures(req, res) {
        // The form's action is '/' and its method is 'POST',
        // so the `app.post('/', ...` route will receive the
        // result of our form
        var data = req.query;
        // var data = jsonfile.readFileSync("/home/devansh/Desktop/Node/NodeService/logs/JSON_1500972522824.json");
        //If no data is received, it will send back response 0

        if(!(parseFloat(req.query.lat) < 19.20 && parseFloat(req.query.lat) > 12.60 && parseFloat(req.query.lng) < 84.80 && parseFloat(req.query.lng) > 76.70))
            return res.json('Outside Bbox')
        var checkdams_query = getQuery1For(req.query.lng, req.query.lat, postgresTables["CHECKDAMS"]);
        var borewells_query = getQuery1For(req.query.lng, req.query.lat, postgresTables["BOREWELLS"]);
        var farmponds_query = getQuery1For(req.query.lng, req.query.lat, postgresTables["FARMPONDS"]);
        var mitanks_query = getQuery1For(req.query.lng, req.query.lat, postgresTables["MI_TANKS"]);
        var others_query = getQuery1For(req.query.lng, req.query.lat, postgresTables["OTHER_WC"]);
        var pt_query = getQuery1For (req.query.lng, req.query.lat, postgresTables["PERCU_TANKS"]);

        var farmponds_query0 = getQuery0For( req.query.lng, req.query.lat, postgresTables["FARMPONDS"])

        var farmponds0 = await executeQuery(farmponds_query0);

        var checkdams1 = await executeQuery(checkdams_query);
        var borewells1 = await executeQuery(borewells_query);
        var farmponds1 = await executeQuery(farmponds_query);
        var mitanks1 = await executeQuery(mitanks_query);
        var others1 = await executeQuery(others_query);
        var pt1 = await executeQuery(pt_query);

        farmponds1 = union(farmponds0, farmponds1);

        //Formatting Data to send in response
        var closestWCS = {};
        closestWCS["checkdam_existing"] = checkdams1;
        closestWCS["borewell"] = borewells1;
        closestWCS["farmpond"] = farmponds1;
        closestWCS["mi_tanks"] = mitanks1;
        closestWCS["others"] = others1;
        closestWCS["percolation_tank"] = pt1;

        // console.log(closestWCS);

        // var closestWCS = checkdams.concat(borewells,farmponds, mitanks, others, pt);
        res.json(closestWCS)
    }

    //getQuery0For will give closest structure including zero storage
    function getQuery0For(lng, lat, tablename) {
        if (tablename === postgresTables["FARMPONDS"]) {
            return 'select "external_id","capacity", "new_villag", "dsply_n", "dname_1",' +
                ' "latitude", "longitude", "iwm_storag", \'FARMPONDS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
                'from "' + postgresTables["FARMPONDS"] + '" ' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 1'
        }
        return -1;
    }

    //getQuery1For will give closest structures with storage greater than 0
    function getQuery1For(lng, lat, tablename) {

        if (tablename === postgresTables["CHECKDAMS"]) {
            return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
                ' "latitude", "longitude", "iwm_storag", \'CHECKDAMS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
                'from "' + postgresTables["CHECKDAMS"] + '" ' +
                'where CAST("iwm_storag" as DECIMAL) > 0 and CAST(iwm_storag as Decimal) > CAST(capacity as DECIMAL) * 0.20' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3'
        }
        else if (tablename === postgresTables["BOREWELLS"]) {
            return 'select "external_id", "pump_capac", "new_villag", "dsply_n", "dname_1", ' +
                '"latitude", "longitude", "iwm_wcs_id", \'BOREWELLS\' as "type"' +
                'from "' + postgresTables["BOREWELLS"] + '"' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3';
        }
        else if (tablename === postgresTables["FARMPONDS"]) {
            return 'select "external_id","capacity", "new_villag", "dsply_n", "dname_1",' +
                ' "latitude", "longitude", "iwm_storag", \'FARMPONDS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
                'from "' + postgresTables["FARMPONDS"] + '" ' +
                'where CAST("iwm_storag" as DECIMAL) > 0 and CAST(iwm_storag as Decimal) > CAST(capacity as DECIMAL) * 0.20' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3'
        }
        else if (tablename === postgresTables["MI_TANKS"]) {
            return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
                ' "latitude", "longitude", "iwm_storag", "iwm_timest", "iwm_image_", \'MI_TANKS\' as "type", "iwm_wcs_id" ' +
                'from "' + postgresTables["MI_TANKS"] + '"' +
                'where CAST("iwm_storag" as DECIMAL) > 0 and CAST(iwm_storag as Decimal) > CAST(capacity as DECIMAL) * 0.20' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3'
        }
        else if (tablename === postgresTables["OTHER_WC"]) {
            return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
                ' "longitude", "latitude", "iwm_storag", \'OTHERS_WC\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
                'from "' + postgresTables["OTHER_WC"] + '"' +
                'where CAST("iwm_storag" as DECIMAL) > 0 and CAST(iwm_storag as Decimal) > CAST(capacity as DECIMAL) * 0.20' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3'
        }
        else if (tablename === postgresTables["PERCU_TANKS"]) {
            return 'select "external_id", "capacity", "new_villag", "dsply_n",' +
                ' "dname_1", "longitude", "latitude", \'PERCULATION_TANK\' as "type", "iwm_wcs_id" ' +
                'from "' + postgresTables["PERCU_TANKS"] + '" ' +
                'where CAST("iwm_storag" as DECIMAL) > 0 and CAST(iwm_storag as Decimal) > CAST(capacity as DECIMAL) * 0.20' +
                'order by "geom" <-> st_setsrid(st_makepoint(' + lng + ',' + lat + '),4326) ' +
                'limit 3'
        }
        return '-1';
    }

    //If there is some Promise unhandled exception thrown, please check the logs Log_WCSDataService<Date>.txt file for error logs. Error along with the query
    //can be checked there.
    async function executeQuery(query) {
        try {
            var result = await client.query(query);
        } catch (err) {
            logError('wcsDataService: Error In Select Query: ' + err + ' Query: ' + query);
        }
        if (result.rowCount == 0) {
            logError('wcsDataService: Problem in Table, 0 rows fetched \n' + query);
        }
        var data = [];
        for (var row in result.rows){
            var single_row = result.rows[row];
            //Structuring the Data
            single_row = ((data) => {
            var new_data = {};
            for (var key in data){
                if(isNaN(parseFloat(data[key])))
                    new_data[postgresAttributes[key]] = data[key];
                else
                    new_data[postgresAttributes[key]] = parseFloat(data[key]);
            }
            return new_data;
        })(single_row);
            data.push(single_row);
        }
        return data;
        // console.log('Data fetched: ' + result.rowCount);
        // console.log(result.rows);
    }

    function union(farmponds0, farmponds1) {
        if (farmponds0[0]['iwm_wcs_id'] != farmponds1[0]['iwm_wcs_id']) {
            farmponds1.unshift(farmponds0[0]);
            farmponds1.splice(3, 1);
        }
        return farmponds1;
    }

    //Given the JSON data, it parses the dailyStorageData and takes timestamp, storage and source type from the string and returns
    function getFormattedRowData(data) {
        if (data.hasOwnProperty('dailyStorageData')) {
            var row = [];
            var latestData = data.dailyStorageData.toString().split(',');
            var arr1 = latestData[latestData.length - 1].toString().split('=');
            // var date = parseDate(arr1[0]);
            //Parses the date for being a valid string and returns Date object.
            var date = ((str) => {
                var y = str.substr(0, 4),
                    m = str.substr(4, 2) - 1,
                    d = str.substr(6, 2);
                var D = new Date(y, m, d);
                return (D.getFullYear() == y && D.getMonth() == m && D.getDate() == d) ? D : 'invalid date';
            })(arr1[0]);
            var curDate = new Date();
            var timeDiff = Math.abs(curDate.getTime() - date.getTime());
            var diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));
            if (diffDays > 30 || date == "invalid date")
                return -1;
            (arr1.length >= 1) ? row.push(arr1[0]) : row.push(null);
            var arr2 = arr1[1].toString().split('#');
            (arr2.length >= 1) ? row.push(arr2[0]) : row.push(null);
            data.hasOwnProperty('imageURL') ? row.push(data.imageURL) : row.push(null);
            (arr2.length == 2) ? row.push(arr2[1]) : row.push(null);
            return row;
        } else {
            return -1;
        }
    }

    //Function to insert rows in the tables with the data received in the API.
    function insertWCSData(id, rowdata) {
        var querystmt = 'UPDATE "' + postgresTables['IWM_DATA'] + '" SET iwm_timest =\'' + rowdata[0] + '\' , iwm_storag = \'' +
            rowdata[1] + '\' , iwm_image_ = \'' + rowdata[2] + '\' , source_type = \'' + rowdata[3] + '\' WHERE iwm_wcs_id = \'' + id + '\';';
        var query = client.query(querystmt, function (err, result) {
            if (err) {
                logError('wcsDataService: Unable to run the query \n' + querystmt);
                logError(err);
                //  console.log('wcsDataService: ' + err);
            } else {
                // console.log('wcsDataService: ' + new Date().toString() + ": Rows Updated: " + result.rowCount);
                if (result.rowCount == 0)
                    logError('wcsDataService: Not Changed for: ' + id + ' does not exist. \n');
            }
        });
    }

    //Function to lof error in the file format (Log_Date.txt) and the error text passed as parameter to the function.
    function logError(error) {
        var file = './logs/Log_WCSDataService' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
        fs.appendFile(file, error + '\n', function (err) {
            if (err) {
                console.log('wcsDataService: ' + err);
            }
            // else
            // console.log('wcsDataService: The Error file was saved!');
        });
    }

    var postgresTables = {
        "CHECKDAMS": "check_dams_view",
        "FARMPONDS": "farm_ponds_view",
        "BOREWELLS": "borewells_view",
        "CHECKDAMS_P": "checkdam_proposed_view",
        "MI_TANKS": "mi_tanks_view",
        "PERCU_TANKS": "pt_view",
        "OTHER_WC": "others_view",
        "IWM_DATA": "iwm_data"
    }

    var postgresAttributes = {
        "external_id": "externalID",
        "capacity": "storageCapacity",
        "pump_capac": "storageCapacity",
        "new_villag": "villageName",
        "dsply_n": "mandalName",
        "dname_1": "districtName",
        "longitude": "longitude",
        "latitude": "latitude",
        "type": "type",
        "iwm_wcs_id": "waterStructureID",
        "source_type": "sourceType",
        "iwm_image_": "imageURL",
        "iwm_timest": "eventGenDay",
        "iwm_storag": "storageValue",
        "ca_sq_km": "catchmentArea"
    }

    return service;
}
