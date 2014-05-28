var getMetricName = function(settings, key){
    var server_name = 'server_' + settings.mysql.host.split('.').join('-');
    return server_name + '.' + key;
};

module.exports = {
    getMetricName: getMetricName
}