var shared = require('./shared.js');
var _ = require('lodash');

var data = null;

var lastValues = {};

var start = function(_data)
{
    console.log("master status started");
    data = _data;


    poll();
}

var poll = function()
{
    if(data.settings.watcher.master != true)
    {
        return;
    }

    var mysql = data.connections.mysql;
    var statsd = data.connections.statsd;
    
    var startMs = Date.now();
    console.log('master status query started');

    mysql.query("SHOW MASTER STATUS", function(err, rows){
        process(rows);
        var ms = Date.now() - startMs;

        console.log('master status query finished: ' + ms.toString());
        statsd.timing(shared.getMetricName(data.settings, 'watcher.poll_masterstatus'), ms);
        setTimeout(poll, data.settings.watcher.pollInterval);
    });
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
        // Check to make sure we have something to check against
        if(typeof lastValues['Position'] != "undefined")
        {
            var diff = val - lastValues['Position'];
            lastValues['Position'] = val;

            statsd.gauge(shared.getMetricName(data.settings, 'master_status.diff.Position'), diff);    
        }
        else
        {
            lastValues['Position'] = val;
        }

        statsd.gauge(shared.getMetricName(data.settings, 'master_status.current.Position'), val);    
    });
}

module.exports = {
    start: start
};