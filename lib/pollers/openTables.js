var shared = require('./shared.js');
var _ = require('lodash');

var data = null;

var start = function(_data)
{
    console.log("open tables started");
    data = _data;


    poll();
}

var poll = function()
{
    var mysql = data.connections.mysql;
    var statsd = data.connections.statsd;
    
    var startMs = Date.now();
    console.log('open tabled query started');

    mysql.query("SHOW OPEN TABLES", function(err, rows){
        process(rows);
        var ms = Date.now() - startMs;

        console.log('open tabled query finished: ' + ms.toString());

        statsd.timing(shared.getMetricName(data.settings, 'watcher.poll_opentables'), ms);
        setTimeout(poll, data.settings.watcher.pollInterval);
    });
}

var process = function(rows)
{
    var statsd = data.connections.statsd;

    // Setup variables to store results in
    var total_open_tables = 0;
    var total_in_use = 0;
    var total_locked = 0;

    // Iterate over the rows
    _.forEach(rows, function(row){
        total_open_tables++;
        total_in_use += row.In_use;
        total_locked += row.Name_locked;
        
        statsd.gauge(shared.getMetricName(data.settings, 'open_tables.' + row.Database + '.' + row.Table + '.in_use'), row.In_use);    
        statsd.gauge(shared.getMetricName(data.settings, 'open_tables.' + row.Database + '.' + row.Table + '.name_locked'), row.Name_locked);
    });

    statsd.gauge(shared.getMetricName(data.settings, 'open_tables.total_in_use'), total_in_use);    
    statsd.gauge(shared.getMetricName(data.settings, 'open_tables.total_name_locked'), total_locked);
}

module.exports = {
    start: start
};