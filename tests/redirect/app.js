// curl -d '{"org":"test1", "project":"test2"}' -H "Content-Typeapplication/json" -X POST http://hub-redirect:4000/v0.000/get_database_fqdn
// curl -d '{"org":"test1", "project":"test2"}' -H "Content-Typeapplication/json" -X POST http://fakeminio.datajoint.io:4000/v0.000/get_database_fqdn
// curl -d '{"org":"test1", "project":"test2"}' -H "Content-Typeapplication/json" -X POST https://fakeminio.datajoint.io/v0.000/get_database_fqdn
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
                    host = "db:3306";
                } else {
                    host = "db:3307";
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
    } else {
        res.writeHead(501);
        res.end();
    }
}
const server = http.createServer(requestListener);
server.listen(4000);