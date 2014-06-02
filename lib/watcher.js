var mysql = require('mysql');
var conn = null;

var watcherSettings = null;

var watch = function(settings){
    watcherSettings = settings;

    // Create connection
    connect(settings, startPolling);
};

var connect = function(settings, callback)
{
    console.log("settings", settings.mysql);
    conn = mysql.createConnection(settings.mysql);
    conn.connect(function(err){
        if(err) {
            console.error('Error occured connecting: ' + err.stack);
            return;
        }

        console.log("Connected!");
        callback(settings);
    });

}

var createStatsdClient = function(settings){
    var SDC = require('statsd-client');
    var sdc = new SDC(settings.statsd);
    
    return sdc;
}

var startPolling = function()
{
    // Setup Shared Object
    var info = {
        connections: {
            mysql: conn,
            statsd: createStatsdClient(watcherSettings)
        },
        settings: watcherSettings,
        run: true
    };

    var processList = require('./pollers/processList.js');
    processList.start(info);

    var openTables = require('./pollers/openTables.js');
    openTables.start(info);

    var globalStatus = require('./pollers/globalStatus.js');
    globalStatus.start(info);

    if(watcherSettings.watcher.master)
    {
        var masterStatus = require('./pollers/masterStatus.js');
        masterStatus.start(info);    
    }
    

    if(watcherSettings.watcher.slave)
    {   
        var slaveStatus = require('./pollers/slaveStatus.js');
        slaveStatus.start(info);
    }
    
}

module.exports = {
    watch: watch
}