'use strict';

var jsonfile = require('jsonfile'),
    fs = require('fs');

module.exports = function (client) {

    var service = {
        getAssociationData: getAssociationData,
        getNNearestVillages: getNNearestVillages
    }

    async function getAssociationData(req, res) {

        // console.log('getAssociationData: ' + req);
        // The form's action is '/' and its method is 'POST',
        // so the `app.post('/', ...` route will receive the
        // result of our form
        var data = req.body;
        // var data = jsonfile.readFileSync("/home/devansh/\Desktop/Node/NodeService/logs/JSON_1500972522824.json");
        //If no data is received, it will send back response 0
        if(req.headers['content-type'] != 'application/json')
            return res.json('-1')
        if (Object.keys(data).length == 0)
            return res.send('-4');
        // else
        // console.log('getAssociationData: ' + new Date().toString() + ": Data Received");
        // var file = 'logs/JSON_layerService' + new Date().getTime().toString() + '.json';
        // jsonfile.writeFile(file, data, function(err) {
        // if (err)
        // console.log('getAssociationData: ' + err);
        // });
        var latlngs = getFormattedData(data);
        if (latlngs == -1)
            return res.send('-3');
        var latlngData = {};
        for (var i = 0; i < latlngs.length; i++) {
            var completeData
            if(validateBbox(latlngs[i]))
                completeData = await fetchFromDBAssociation(latlngs[i]);
            else
                completeData = '0'
            //  if(completeData != -1)
            latlngData[latlngs[i]] = completeData;
        }
        res.json(latlngData);
        // console.log('getAssociationData: ' + new Date().toString() + ": Data Delivered.");
    }

    async function getNNearestVillages(req, res){
        // console.log('getNNearestVillages: ' + req);
        // The form's action is '/' and its method is 'POST',
        // so the `app.post('/', ...` route will receive the
        // result of our form
        var data = req.body;
        // var data = jsonfile.readFileSync("/home/devansh/\Desktop/Node/NodeService/logs/JSON_1500972522824.json");
        //If no data is received, it will send back response 0
        if(req.headers['content-type'] != 'application/json')
            return res.json('-1')

        if (Object.keys(data).length == 0)
            return res.send("-4");

        var latlngs = getFormattedData(data);
        var count = ((d) => {
            if(d.hasOwnProperty('count'))
                return d['count'];
            else
                return 1;
        })(data);
        if (latlngs == -1)
            return res.send('-3');
        var latlngData = {};
        for (var i = 0; i < latlngs.length; i++) {
            var completeData
            if(validateBbox(latlngs[i]))
                completeData = await fetchFromDBVillages(latlngs[i], count);
            else
                completeData = '0'
            //  if(completeData != -1)
            latlngData[latlngs[i]] = completeData;
        }
        res.json(latlngData);
    }

    function validateBbox(latlng){
        return parseFloat(latlng[0]) < 19.20 && parseFloat(latlng[0]) > 12.60 && parseFloat(latlng[1]) < 84.80 && parseFloat(latlng[1]) > 76.70
    }

    async function fetchFromDBVillages(latlng, count){
        //Below query is to get nearest Village Map(Along with microbasin, subbasin, basin)

        // SELECT dmv_code, dname as "District", dsply_n_1 as "Mandal", new_villag as "Village", old_villag as "Old_ShapefileName",
        // "AP_Microbasin".dsply_n as "Micro_Basin", basin_name as "Basin", mi_basin as "Sub_Basin"  FROM 
        // "AP_Village" JOIN "AP_Microbasin" on  ST_contains("AP_Microbasin".geom,ST_SetSRID(ST_POINTONSURFACE("AP_Village".geom),4326))
        // ORDER BY "AP_Village".geom <-> st_setsrid(ST_Point(77.37,15.16),4326) limit 3;

        var querystmt = 'SELECT dmvcode, iwmdistric as '+ 
        '"District", iwmmandal as "Mandal", iwmvillage as "Village", oldvillage as "Old_ShapefileName"' +
        ' FROM "ap_village" ' + 
        'where EXISTS(SELECT "dmv_code" FROM "ap_village"  where ST_contains(geom,ST_SetSRID(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326)))' + 
        'order by  geom <-> st_setsrid(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326) limit ' + count + ';';
         try {
            var nearestVillageData = await client.query(querystmt);
        } catch (err) {
            // console.log('getNNearestVillages: Error while executing query\n ' + err);
            logError('getNNearestVillages: Error while executing query for Nearest Villages\n ' + err);
            return '-1';
        }
        if(nearestVillageData.rowCount == 0)
            return '0'
        var listOfVillages = [];
        for(var row in nearestVillageData.rows){
            listOfVillages.push(nearestVillageData.rows[row]);
        }
        return listOfVillages;
    }

    async function fetchFromDBAssociation(latlng) {
        var querystmt = prepareQuery(latlng);
        //  console.log('getAssociationData: ' + querystmt);
        try {
            var villageData = await client.query(querystmt[0]);
        } catch (err) {
            //console.log('getAssociationData: Error while executing query\n ' + err);
            logError('getAssociationData: Error while executing query for Association\n ' + err);
            return '-1';
        }
        try {
            var basinData = await client.query(querystmt[1]);
        } catch (err) {
            // console.log('layerDataService: Error while executing query\n ' + err);
            logError('layerDataService: Error while executing query for Association\n ' + err);
            return '-1';
        }

        if (villageData.rowCount > 1) {
            logError('getAssociationData: More than one Villages found for a lattitude and longitude (Query)\n' + querystmt[0]);
            // console.log("getAssociationData: More than one Villages found for a lattitude and longitude (Query)\n" + querystmt[0]);
            return 'Conflict: More than 1 vilalge'
        } else if (villageData.rowCount == 0) {
            logError('getAssociationData: No Village found for the given lattitude and longitude (Query)\n' + querystmt[0]);
            // console.log("getAssociationData: No Village found for the given lattitude and longitude (Query)\n" + querystmt[0]);
            return '0';
        } else if (basinData.rowCount > 1) {
            logError('getAssociationData: More than one basin found for a lattitude and longitude (Query)\n' + querystmt[1]);
            // console.log("getAssociationData: More than one basin found for a lattitude and longitude (Query)\n" + querystmt[1]);
            return 'Conflict: More than 1 Basin'
        } else if (basinData.rowCount == 0) {
            logError('getAssociationData: No basin found for the given lattitude and longitude (Query)\n' + querystmt[1]);
            // console.log("getAssociationData: No basin found for the given lattitude and longitude (Query)\n" + querystmt[1]);
            return '0';
        }
        var finalData = Object.assign({}, villageData.rows['0'], basinData.rows['0']);
        return finalData;
    }

    function getFormattedData(data) {
        var latlngs = [];
        var latlngMap = [];
        if (data.hasOwnProperty('latlng'))
            var latlngs = data['latlng'].split('#');
        else {
            logError('getNNearestVillages: No latlng property is present in the request. ' + data + '\n');
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
        query.push('SELECT dmvcode, iwmdistric as "District", iwmmandal as "Mandal", iwmvillage as "Village", oldvillage as "Old_ShapefileName" FROM "' +
            'ap_village' + '"  where ST_contains(geom,ST_SetSRID(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326))');
        query.push('SELECT dsply_n as "Micro_Basin", basin_name as "Basin", mi_basin as "Sub_Basin" FROM "' +
            'ap_microbasin' + '" where ST_contains(geom,ST_SetSRID(ST_Point(' + latlng[1] + ',' + latlng[0] + '),4326))');
        return query;
    }

    //Function to lof error in the file format (Log_Date.txt) and the error text passed as parameter to the function.
    function logError(error) {
        var file = './logs/Log_LayerService' + new Date().toJSON().slice(0, 10).replace(/-/g, '_') + '.txt';
        fs.appendFile(file, error + '\n', function (err) {
            if (err) {
                console.log('layerDataService: ' + err);
            }
            // else
            // console.log('layerDataService: The Error file was saved!');
        });
    }
    return service;
}
