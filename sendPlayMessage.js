const winston = require('winston');
const amqp = require('amqplib');
const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectID;
const QUEUE_NAME = 'wat_queue'

const dbUrl = `mongodb://${serverNames.mongoServerName}:27017/wat_storage`;
const rmqUrl = `amqp://${serverNames.rabbitServerName}`;

const sid = process.argv[2];

playNow(sid);

function playNow(sid) {
	MongoClient.connect(dbUrl).then(db => {
		db.collection('scenario', (err, scenarioCollection) => {
			if (err) {
				res.send(err).status(404).end();
			} else {
				var p1 = scenarioCollection.find({_id:sid}).toArray();
				var p2 = amqp.connect(rmqUrl).then( q => {return q.createChannel()}).catch( e=> {return Promise.reject(e)});
				Promise.all([p1,p2]).then(values => {
					var scenarioToPlay = values[0];
					var channel = values[0];
					channel.assertQueue(QUEUE_NAME, { durable: true });
					channel.sendToQueue(QUEUE_NAME,
						Buffer.from(JSON.stringify({ actions: scenarioToPlay.actions })), { persistent: true });
						winston.log(`play request sent for scenario ${sid}`);
				})
				.catch(err => {
					winston.error(err);
				})
			}
		});
		db.close();
	}).catch(err => {
		winston.info(err);
		winston.error(err);
	});
}

	