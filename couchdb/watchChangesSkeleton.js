var http = require('http'), events = require('events');

exports.createWatcher = function(options) {
    var watcher = new events.EventEmitter();
    watcher.host = options.host || 'localhost';
    watcher.port = options.port || 5984;
    watcher.last_seq = options.last_seq || 0;
    watcher.db = options.db || '_users';
    watcher.username = options.username || 'admin';
    watcher.password = options.password || 'admin';
    watcher.start = function() {
        var auth = 'Basic ' + Buffer.from(watcher.username + ':' + watcher.password).toString('base64');
        var httpOptions = {
            host: watcher.host,
            port: watcher.port,
            path: '/' +
                watcher.db +
                '/_changes' +
                '?feed=longpoll&include_docs=true&since=' +
                watcher.last_seq,
            headers: {
                'Authorization': auth
            }
        };

        http.get(httpOptions, function(res) {
            var buffer = '';
            res.on('data', function (chunk) {
                buffer += chunk;
            });
            res.on('end', function() {
                var output = JSON.parse(buffer);
                if (output.results) {
                    watcher.last_seq = output.last_seq;
                    output.results.forEach(function(change){
                        watcher.emit('change', change);
                    });
                } else {
                    watcher.start();
                    watcher.emit('error', output);
                }
            });
        }).on('error', function(err) {
            watcher.emit('error', err);
        });
    };
    return watcher;
};
if (!module.parent) {
    exports.createWatcher({
        db: process.argv[2],
        last_seq: process.argv[3]
    })
        .on('change', console.log)
        .on('error', console.error)
        .start();
}