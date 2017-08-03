'use strict';

var jsonfile = require('jsonfile'),
    fs = require('fs');

module.exports = function(client) {
	
    var service = {
        getAssociationData: getAssociationData
    }

    async function getAssociationData(req, res) {
		
        // console.log('layerDataService: ' + req);
        // The form's action is '/' and its method is 'POST',
        // so the `app.post('/', ...` route will receive the
        // result of our form
        var data = req.body;
        // var data = jsonfile.readFileSync("/home/devansh/\Desktop/Node/NodeService/logs/JSON_1500972522824.json");
        //If no data is received, it will send back response 0

        if (Object.keys(data).length == 0)
            return res.send("0");
        // else
            // console.log('layerDataService: ' + new Date().toString() + ": Data Received");
        // var file = 'logs/JSON_layerService' + new Date().getTime().toString() + '.json';
        // jsonfile.writeFile(file, data, function(err) {
            // if (err)
                // console.log('layerDataService: ' + err);
        // });
        var latlngs = getFormattedData(data);
        if(latlngs == -1)
            return res.send('-1');
        var latlngData = {};
        for (var i = 0; i < latlngs.length; i++) {
            var completeData = await fetchFromDB(latlngs[i]);
            //  if(completeData != -1)
            latlngData[latlngs[i]] = completeData;
        }
        res.json(latlngData);
        // console.log('layerDataService: ' + new Date().toString() + ": Data Delivered.");
    }

    async function fetchFromDB(latlng) {
        var querystmt = prepareQuery(latlng);
        //  console.log('layerDataService: ' + querystmt);
        try {
            var villageData = await client.query(querystmt[0]);
        } catch (err) {
            //console.log('layerDataService: Error while executing query\n ' + err);
			logError('layerDataService: Error while executing query\n ' + err);
            return '-1';
        }
        try{
            var basinData = await client.query(querystmt[1]);
        } catch(err){
			// console.log('layerDataService: Error while executing query\n ' + err);
			logError('layerDataService: Error while executing query\n ' + err);
			return '-1';
        }

        if (villageData.rowCount > 1) {
            logError("layerDataService: More than one Villages found for a lattitude and longitude (Query)\n" + querystmt[0]);
            // console.log("layerDataService: More than one Villages found for a lattitude and longitude (Query)\n" + querystmt[0]);
            return '-2'
        } else if (villageData.rowCount == 0) {
            logError("layerDataService: No Village found for the given lattitude and longitude (Query)\n" + querystmt[0]);
            // console.log("layerDataService: No Village found for the given lattitude and longitude (Query)\n" + querystmt[0]);
            return '0';
        }else if(basinData.rowCount > 1){
          logError("layerDataService: More than one Villages found for a lattitude and longitude (Query)\n" + querystmt[1]);
          // console.log("layerDataService: More than one Villages found for a lattitude and longitude (Query)\n" + querystmt[1]);
          return '-2'
        } else if(basinData.rowCount == 0){
          logError("layerDataService: No Village found for the given lattitude and longitude (Query)\n" + querystmt[1]);
          // console.log("layerDataService: No Village found for the given lattitude and longitude (Query)\n" + querystmt[1]);
          return '0';
        }
        var finalData = Object.assign({},villageData.rows['0'],basinData.rows['0']);
        return finalData;
    }

    function getFormattedData(data) {
        var latlngs = [];
        var latlngMap = [];
        if(data.hasOwnProperty('latlng'))
            var latlngs = data['latlng'].split('#');
        else{
            logError("layerDataService: No latlng property is present in the request. " + data);
            return -1;
        }
        for (var i = 0; i < latlngs.length; i++) {
            var p = [];
            var point = latlngs[i].toString().split(',');
            p.push(point[0]);
            p.push(point[1]);
            latlngMap.push(p);
        }
        return latlngMap;
    }

    function prepareQuery(latlng) {
        var query = [];
        query.push('SELECT dmv_code, dname as "District", dsply_n_1 as "Mandal", new_villag as "Village", old_villag as "Old_ShapefileName" FROM "' +
            'ap_village' + '"  where ST_contains(geom,ST_SetSRID(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326))');
        query.push('SELECT dsply_n as "Micro_Basin", basin_name as "Basin", mi_basin as "Sub_Basin" FROM "'  +
                    'ap_microbasin' + '" where ST_contains(geom,ST_SetSRID(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326))');
        return query;
    }

    //Function to lof error in the file format (Log_Date.txt) and the error text passed as parameter to the function.
    function logError(error) {
        var file = './logs/Log_LayerService' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
        fs.appendFile(file, error + '\n', function(err) {
            if (err) {
                console.log('layerDataService: ' + err);
            }
            // else
            // console.log('layerDataService: The Error file was saved!');
        });
    }
    return service;
}
