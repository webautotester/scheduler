const winston = require('winston');
const amqp = require('amqplib');
const MongoClient = require('mongodb').MongoClient;
const ObjectId = require('mongodb').ObjectID;
const Crontab = require('crontab');
const QUEUE_NAME = 'wat_queue'

module.exports.init = function(serverNames, webServer) {
	const dbUrl = `mongodb://${serverNames.mongoServerName}:27017/wat_storage`;
	const rmqUrl = `amqp://${serverNames.rabbitServerName}`;
	webServer.post('/play', (req, res) => {
		const playOptions = req.body;
		if (playOptions.now) {
			playNow(req,res, playOptions);
		} else {
			addCron(req, res, playOptions)
		}
	});

	function playNow(req, res, playOptions) {
		MongoClient.connect(dbUrl).then(db => {
			db.collection('scenario', (err, scenarioCollection) => {
				if (err) {
					res.send(err).status(404).end();
				} else {
					var p1 = scenarioCollection.find({_id:playOptions.sid}).toArray();
					var p2 = amqp.connect(rmqUrl).then( q => {return q.createChannel()}).catch( e=> {return Promise.reject(e)});
					Promise.all([p1,p2]).then(values => {
						var scenarioToPlay = values[0];
						var channel = values[0];
						channel.assertQueue(QUEUE_NAME, { durable: true });
						channel.sendToQueue(QUEUE_NAME,
							Buffer.from(JSON.stringify({ actions: scenarioToPlay.actions })), { persistent: true });
							res.send(`play request sent for scenario ${playOptions.sid}`).status(200).end();
					})
					.catch(err => {
						res.send(err).status(500).end();
					})
				}
			});
			db.close();
		}).catch(err => {
			winston.info(err);
			res.send(err).status(500).end;
		});
	}

	function addCron(req, res, playOptions) {
		const cron = playOptions.cron;
		if (cron) {
			// create with string expression
			var job = Crontab.create(`node /tmp/scheduler/sendPlayMessage.js ${playOptions.sid}`, cron);
			Crontab.save(function(err, newCrontab) {
				if (err) {
					res.send(err).status(500).end();
				}
				else {
					res.send(`play request sent for scenario ${playOptions.sid}`).status(200).end();
				}
			}); 
		} else {
			res.send(`cron ${cron} is not valid`).status(500).end();
		}
	}
};