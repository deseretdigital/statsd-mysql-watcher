var shared = require('./shared.js');
var _ = require('lodash');

var data = null;

var start = function(_data)
{
    data = _data;


    poll();
}

var poll = function()
{
    console.log('process list query started');
    var mysql = data.connections.mysql;
    var statsd = data.connections.statsd;

    var startMs = Date.now();

    mysql.query("SHOW PROCESSLIST", function(err, rows){

        process(rows);

        var ms = Date.now() - startMs;

        console.log('process list query finished: ' + ms.toString());

        statsd.timing(shared.getMetricName(data.settings, 'watcher.poll_processList'), ms);
        setTimeout(poll, data.settings.watcher.pollInterval);
    });
}

var process = function(rows)
{
    var statsd = data.connections.statsd;

    // Setup variables to store results in
    var dbs = {"no_db": 0};    
    var users = {}

    // Iterate over the rows
    _.forEach(rows, function(row){
        // Which DB are queries executing against
        var database_name = '';
        if(row.db)
        {
            database_name = "db_" + row.db;
        }
        else
        {
            database_name = "_no-db";
        }

        if(!dbs[database_name])
        {
            dbs[database_name] = 0;
        }

        dbs[database_name]++;

        // Usernames currently connected
        var user_name = row.User;

        if(!users[user_name])
        {
            users[user_name] = 0;
        }

        users[user_name]++;
    });

    statsd.gauge(shared.getMetricName(data.settings, 'processlist.num_of_dbs'), Object.keys(dbs).length);
    _.forEach(dbs, function(val, key){
        statsd.gauge(shared.getMetricName(data.settings, 'processlist.dbs.' + key), val);
    });

    statsd.gauge(shared.getMetricName(data.settings, 'processlist.num_of_users'), Object.keys(users).length);

    _.forEach(users, function(val, key){
        statsd.gauge(shared.getMetricName(data.settings, 'processlist.users.' + key), val);
    });
}

module.exports = {
    start: start
};