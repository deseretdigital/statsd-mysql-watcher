var shared = require('./shared.js');
var _ = require('lodash');

var data = null;

var lastValues = {};

var start = function(_data)
{
    console.log("slave status started");
    data = _data;


    poll();
}

var poll = function()
{
    if(data.settings.watcher.slave != true)
    {
        return;
    }

    var mysql = data.connections.mysql;
    var statsd = data.connections.statsd;
    
    var startMs = Date.now();
    console.log('slave status query started');

    mysql.query("SHOW SLAVE STATUS", function(err, rows){
        process(rows);
        var ms = Date.now() - startMs;

        console.log('slave status query finished: ' + ms.toString());
        statsd.timing(shared.getMetricName(data.settings, 'watcher.poll_slavestatus'), ms);
        setTimeout(poll, data.settings.watcher.pollInterval);
    });
}

var processValue = function(statsd, key, val)
{
    if(val == 'Yes')
    {
        val = 1;
    }
    else if (val == 'No')
    {
        val = 0;
    }

    var val = parseInt(val);
    if(typeof val != "number" || val.toString() == 'NaN')
    {
        return;
    }

    // Check to make sure we have something to check against
    if(typeof lastValues[key] != "undefined")
    {
        var diff = val - lastValues[key];
        lastValues[key] = val;

        statsd.gauge(shared.getMetricName(data.settings, 'slave_status.diff.' + key), diff);    
    }
    else
    {
        lastValues[key] = val;
    }

    statsd.gauge(shared.getMetricName(data.settings, 'slave_status.current.' + key), val);    
}

var process = function(rows)
{
    var statsd = data.connections.statsd;

    if(rows.length <= 0)
    {
        return;
    }

    // For the first row (and only row)
    _.first(rows, function(row){
        var val = row.Position;
        console.log(row);
        _.forEach(row, function(val, key){
            processValue(statsd, key, val);
        });
    });
}

module.exports = {
    start: start
};