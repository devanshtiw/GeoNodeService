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
      var mitanks_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["MITANKS"]);
      var others_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["OTHERS"]);
      var pt_query = getQuery1For(req.query.lat,req.query.lng,postgresTables["PT"]);

      var farmponds_query0 = getQuery0For(req.query.lat, req.query.lng, postgresTables["FARMPONDS"])

      var farmponds0 = await executeQuery(farmponds_query0);

      var checkdams = await executeQuery(checkdams_query);
      var borewells = await executeQuery(borewells_query);
      var farmponds = await executeQuery(farmponds_query);
      var mitanks = await executeQuery(mitanks_query);
      var others = await executeQuery(others_query);
      var pt = await executeQuery(pt_query);


      farmponds = union(farmponds0, farmponds);

      var closestWCS = checkdams.concat(borewells,farmponds, mitanks, others, pt);
      console.log(closestWCS.length);
      res.json(closestWCS)

    }

    function getQuery0For(lat, lng, tablename){
      if(tablename === postgresTables["FARMPONDS"]){
        return 'select "external_id","capacity", \'FARMPONDS\' as "type", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"'+
        'from "' + postgresTables["FARMPONDS"] + '" ' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) '+
        'limit 1'
      }
      return -1;
    }

    function getQuery1For(lat, lng, tablename){
      if(tablename === postgresTables["CHECKDAMS"]){
        return 'select "external_id", "capacity", \'CHECKDAMS\' as "type", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"' +
        'from "' + postgresTables["CHECKDAMS"] + '" ' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(' + lat + ',' + lng + '),4326) ' +
        'limit 3'
      }
      else if (tablename === postgresTables["BOREWELLS"]){
        return 'select "external_id", "pump_capac", \'BOREWELLS\' as "type", "new_villag", "dsply_n", "dname_1", ' +
        '"latitude", "longitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"' +
        'from "' + postgresTables["BOREWELLS"] + '"' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326)'+
        'limit 3';
      }
      else if(tablename === postgresTables["FARMPONDS"]){
        return 'select "external_id","capacity", \'FARMPONDS\' as "type", "new_villag", "dsply_n", "dname_1",'
        +' "latitude", "longitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"'+
        'from "' + postgresTables["FARMPONDS"] + '" ' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) '+
        'limit 3'
      }
      else if(tablename === postgresTables["MITANKS"]){
        return 'select "external_id", "capacity", \'MI_TANKS\' as "type", "new_villag", "dsply_n", "dname_1",' +
        ' "latitude", "longitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type" ' +
        'from "' + postgresTables["MITANKS"] + '"' +
        'where CAST("iwm_storag" as DECIMAL) > 0 ' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326) ' +
        'limit 3'
      }
      else if(tablename === postgresTables["OTHERS"]){
        return 'select "external_id", "capacity", \'OTHERS_WC\' as "type", "new_villag", "dsply_n", "dname_1",' +
        ' "longitude", "latitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"' +
        'from "' + postgresTables["OTHERS"] + '"' +
        'where CAST("iwm_storag" as DECIMAL) > 0' +
        'order by "geom" <-> st_setsrid(st_makepoint(18.006973,83.446453),4326)' +
        'limit 3'
      }
      else if(tablename === postgresTables["PT"]){
        return 'select "external_id", "capacity",\'PERCULATION_TANK\' as "type", "new_villag", "dsply_n",' +
        ' "dname_1", "longitude", "latitude", "geom", "iwm_wcs_id", "iwm_storag", "iwm_timest", "iwm_image_", "source_type"' +
        'from "' + postgresTables["PT"] + '" ' +
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
          logError('wcsDataService: Error In Select Query: ' + err);
        }
        if(result.rowCount != 3){
          logError('wcsDataService: Problem in Table, less than 3 rows fetched \n' + query );
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
      "CHECKDAMS_PROPOSED": "checkdam_proposed_view",
      "MITANKS": "mi_tanks_view",
      "PT": "pt_view",
      "OTHERS": "others_view",
      "IWM_DATA": "iwm_data"
    }

    return service;
}
