'use strict';

var jsonfile = require('jsonfile'),
    fs = require('fs');


module.exports = function(client) {

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
        // var data = jsonfile.readFileSync("D:/node/logs/JSON_1501657336949.json");
        //If no data is received, it will send back response 0
        if (Object.keys(data).length == 0)
            return res.send("0");
        else
            console.log('wcsDataService: ' + new Date().toString() + ": Data Received");
        var file = 'logs/JSON_wcsService' + new Date().getTime().toString() + '.json';
        jsonfile.writeFile(file, data, function(err) {
            if (err)
                console.log('wcsDataService: ' + err);
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
                            insertWCSData(id, rowd);
                            rcount++;
                        } else {
                            logError("wcsDataService: ID " + id + " does not have property dailyStorageData");
                        }
                    }
                }
            }
        }
         console.log('wcsDataService: ' + new Date().toString() + ": Rows Processed: " + rcount);
    }

    async function getNearestWCSStructures(req, res){
      // The form's action is '/' and its method is 'POST',
      // so the `app.post('/', ...` route will receive the
      // result of our form
      var data = req.query;
      // var data = jsonfile.readFileSync("/home/devansh/Desktop/Node/NodeService/logs/JSON_1500972522824.json");
      //If no data is received, it will send back response 0

      var checkdams_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["CHECKDAMS"]);
      var borewells_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["BOREWELLS"]);
      var farmponds_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["FARMPONDS"]);
      var mitanks_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["MI_TANKS"]);
      var others_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["OTHER_WC"]);
      var pt_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["PERCU_TANKS"]);

      var farmponds_query0 = getQuery0For(req.query.lat, req.query.lng, postgresTables["FARMPONDS"])

      var farmponds0 = await executeQuery(farmponds_query0);

      var checkdams = await executeQuery(checkdams_query);
      var borewells = await executeQuery(borewells_query);
      var farmponds = await executeQuery(farmponds_query);
      var mitanks = await executeQuery(mitanks_query);
      var others = await executeQuery(others_query);
      var pt = await executeQuery(pt_query);

      farmponds = union(farmponds0, farmponds);

      var closestWCS = {};
      closestWCS["CHECKDAMS"] = checkdams;
      closestWCS["BOREWELLS"] = borewells;
      closestWCS["FARMPONDS"] = farmponds;
      closestWCS["MI_TANKS"] = mitanks;
      closestWCS["OTHER_WC"] = others;
      closestWCS["PERCU_TANKS"] = pt;

      console.log(closestWCS);


      // var closestWCS = checkdams.concat(borewells,farmponds, mitanks, others, pt);
      res.json(closestWCS)
    }

    function getQuery0For(lat, lng, tablename){
      if(tablename === postgresTables["FARMPONDS"]){
        return 'select "external_id","capacity", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "iwm_storag", \'FARMPONDS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" '+
        'from "' + postgresTables["FARMPONDS"] + '" ' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) '+
        'limit 1'
      }
      return -1;
    }

    //If there is some Promise unhandled exception thrown, please check the logs Log_WCSDataService<Date>.txt file for error logs. Error along with the query
    //can be checked there.
    function getQuery1For(lat, lng, tablename){
      if(tablename === postgresTables["CHECKDAMS"]){
        return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "iwm_storag", \'CHECKDAMS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
        'from "' + postgresTables["CHECKDAMS"] + '" ' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(' + lat + ',' + lng + '),4326) ' +
        'limit 3'
      }
      else if (tablename === postgresTables["BOREWELLS"]){
        return 'select "external_id", "pump_capac", "new_villag", "dsply_n", "dname_1", ' +
        '"latitude", "longitude", "iwm_wcs_id", \'BOREWELLS\' as "type"' +
        'from "' + postgresTables["BOREWELLS"] + '"' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326)'+
        'limit 3';
      }
      else if(tablename === postgresTables["FARMPONDS"]){
        return 'select "external_id","capacity", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "iwm_storag", \'FARMPONDS\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" '+
        'from "' + postgresTables["FARMPONDS"] + '" ' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) '+
        'limit 3'
      }
      else if(tablename === postgresTables["MI_TANKS"]){
        return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "iwm_storag", "iwm_timest", "iwm_image_", \'MI_TANKS\' as "type", "iwm_wcs_id" ' +
        'from "' + postgresTables["MI_TANKS"] + '"' +
        'where CAST("iwm_storag" as DECIMAL) > 0 ' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) ' +
        'limit 3'
      }
      else if(tablename === postgresTables["OTHER_WC"]){
        return 'select "external_id", "capacity", "new_villag", "dsply_n", "dname_1",' +
        ' "longitude", "latitude", "iwm_storag", \'OTHERS_WC\' as "type", "ca_sq_km", "iwm_timest", "iwm_image_", "iwm_wcs_id" ' +
        'from "' + postgresTables["OTHER_WC"] + '"' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326)' +
        'limit 3'
      }
      else if(tablename === postgresTables["PERCU_TANKS"]){
        return 'select "external_id", "capacity", "new_villag", "dsply_n",' +
        ' "dname_1", "longitude", "latitude", \'PERCULATION_TANK\' as "type", "iwm_wcs_id" ' +
        'from "' + postgresTables["PERCU_TANKS"] + '" ' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326)' +
        'limit 3'
      }
      return '-1';
    }

    async function executeQuery(query){
        try{
          var result = await client.query(query);
        }catch(err){
          logError('wcsDataService: Error In Select Query: ' + err + ' Query: ' +  query);
        }
        if(result.rowCount == 0){
          logError('wcsDataService: Problem in Table, 0 rows fetched \n' + query );
        }
        var data = [];
        for(var row in result.rows)
          data.push(result.rows[row]);
        return data;
        // console.log('Data fetched: ' + result.rowCount);
        // console.log(result.rows);
      }

      function union(farmponds0, farmponds1){

        if(farmponds0[0]['iwm_wcs_id'] != farmponds1[0]['iwm_wcs_id']){
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

    //Date is being formatted in ascending order(Sorted)
    function dateFormatter(data) {
        var dates = [];
        for (var date in data)
            dates.push(date);
        dates.sort();
        return dates;
    }

    //Function to insert rows in the tables with the data received in the API.
    function insertWCSData(id, rowdata) {
            var querystmt = 'UPDATE "' + posgresTables['IWM_DATA'] + '" SET iwm_timest =\'' + rowdata[0] + '\' , iwm_storag = \'' +
                rowdata[1] + '\' , iwm_image_ = \'' + rowdata[2] + '\' , source_type = \'' + rowdata[3] + '\' WHERE iwm_wcs_id = \'' + id + '\';';
            var query = client.query(querystmt, function(err, result) {
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
        fs.appendFile(file, error + '\n', function(err) {
            if (err) {
                console.log('wcsDataService: ' + err);
            }
            // else
            // console.log('wcsDataService: The Error file was saved!');
        });
    }

    //Parses the date for being a valid string and returns Date object.
    function parseDate(str) {
        var y = str.substr(0, 4),
            m = str.substr(4, 2) - 1,
            d = str.substr(6, 2);
        var D = new Date(y, m, d);
        return (D.getFullYear() == y && D.getMonth() == m && D.getDate() == d) ? D : 'invalid date';
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

    return service;
}
