const http = require('http');
const url = require('url');
var send_correct_response = false;
const requestListener = function (req, res) {
    var host;
    var urlObject = url.parse(req.url, true);
    if (urlObject.pathname.includes('/projects/') && req.method === 'GET') {
        if (urlObject.query.org_name === 'datajoint' && 
                urlObject.pathname.split('/projects/')[1] === 'travis') {
            if (send_correct_response) {
                host = "fakeservices.datajoint.io:3306";
            } else {
                host = "fakeservices.datajoint.io:3307";
                send_correct_response = true;
            }
            res.writeHead(200, {'Content-Type': 'application/json'});
            res.write(`[{"org_name": "${urlObject.query.org_name}", 
                "project_name": "${urlObject.pathname.split('/projects/')[1]}", 
                "database_dsn": "${host}"}]`);
        }
        else {
            res.writeHead(404);
        }
    } else if (urlObject.pathname === '/status' && req.method === 'GET') {
            res.writeHead(200);
    } else {
        res.writeHead(501);
    }
    res.end();
}
const server = http.createServer(requestListener);
server.listen(4000);