var shared = require('./shared.js');
var _ = require('lodash');

var data = null;

var lastValues = {};

var start = function(_data)
{
    console.log("global status started");
    data = _data;


    poll();
}

var poll = function()
{
    var mysql = data.connections.mysql;
    var statsd = data.connections.statsd;
    
    var startMs = Date.now();
    console.log('global status query started');

    mysql.query("SHOW GLOBAL STATUS", function(err, rows){
        process(rows);
        var ms = Date.now() - startMs;

        console.log('global status query finished: ' + ms.toString());
        statsd.timing(shared.getMetricName(data.settings, 'watcher.poll_globalstatus'), ms);
        setTimeout(poll, data.settings.watcher.pollInterval);
    });
}

var process = function(rows)
{
    var statsd = data.connections.statsd;

    // Iterate over the rows
    _.forEach(rows, function(row){
        var val = parseInt(row.Value);
        if(typeof val != "number" || isNaN(parseFloat(val)))
        {
            return;
        }
        
        // Check to make sure we have something to check against
        if(typeof lastValues[row.Variable_name] != "undefined")
        {
            var diff = val - lastValues[row.Variable_name];
            lastValues[row.Variable_name] = val;

            statsd.gauge(shared.getMetricName(data.settings, 'global_status.diff.' + row.Variable_name), diff);    
        }
        else
        {
            lastValues[row.Variable_name] = val;
        }

        statsd.gauge(shared.getMetricName(data.settings, 'global_status.current.' + row.Variable_name), val);    
    });
}

module.exports = {
    start: start
};