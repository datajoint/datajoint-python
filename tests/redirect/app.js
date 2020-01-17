const http = require('http');
const { parse } = require('querystring');
var send_correct_response = false;
const requestListener = function (req, res) {
    if (req.url === '/v0.000/get_database_fqdn' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => {
            body += chunk.toString(); // convert Buffer to string
        });
        req.on('end', () => {
            var payload = parse(body);
            console.log(payload);
            var host;
            if (payload.org === 'datajoint' && payload.project === 'travis') {
                if (send_correct_response) {
                    host = "fakeminio.datajoint.io:3306";
                } else {
                    host = "fakeminio.datajoint.io:3307";
                    send_correct_response = true;
                }
                res.writeHead(200, {'Content-Type': 'application/json'});
                res.write(`{"database.host": "${host}"}`);
            }
            else {
                res.writeHead(404);
            }
            res.end();
        });
    } else if (req.url === '/status' && req.method === 'GET') {
            res.writeHead(200);
            res.end();
    } else {
        res.writeHead(501);
        res.end();
    }
}
const server = http.createServer(requestListener);
server.listen(4000);