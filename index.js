var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client(),
    Consumer = kafka.Consumer,
    KeyedMessage = kafka.KeyedMessage,
    producer = new Producer(client);

const express = require('express')
const app = express();
var bodyParser = require('body-parser');
app.use(bodyParser.json()); // support json encoded bodies
app.use(bodyParser.urlencoded({ extended: true })); // support encoded bodies



console.log('*********UI Node Service(Producer) and (Consumer)*********');


producer.on('ready', function() {
    console.log("Producer is ready to send the payload");
});
consumer = new Consumer(
    client, [
        { topic: 'AuthenticatedQ', partition: 0 }
    ], {
        autoCommit: true
    }
);

//for (var index = 0; index < 10; index++) {
function authenticate(credentials) {

    return new Promise(function(resolve, reject) {

        //for (var index = 0; index < 10; index++) {
        //credentials.userId = Math.random(100);
        var km = new KeyedMessage('key', JSON.stringify(credentials));
        console.log(`Sending User:[${credentials.userName}] for authentication`);

        var payloads = [
            { topic: 'pendingAuthenticationQ', messages: km, partition: 0 }
        ];
        console.log(`Payload is ${payloads}`);
        producer.send(payloads, function(err, data) {
            console.log("Sent Payload..waiting for the reply");
            consumer.on('message', function(message) {
                resolve(message);
            });
        });
    });
}

producer.on('error', function(err) {
    console.log("Error", err);
})



app.post('/authenticate', (req, res) => {
    var crds = {
        userName: req.body.userName,
        userPassword: req.body.userPassword,
        userId: req.body.id
    };
    authenticate(crds).then((data) => {
        var authStatus = JSON.parse(data.value).authenticated;
        return res.send({ "authenticated": authStatus });
    });

});



app.listen(3000, () => console.log('Example app listening on port 3000!'));