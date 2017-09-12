const winston = require('winston');
const amqp = require('amqplib');
const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;
const Crontab = require('crontab');
const QUEUE_NAME = 'wat_queue';

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
		winston.info(`Play Now Request on ${dbUrl}`);
		MongoClient.connect(dbUrl).then(db => {
			db.collection('scenario', (err, scenarioCollection) => {
				if (err) {
					winston.error(`Play Now Request Error : ${err}`);
					db.close();
					res.send(err).status(404).end();
				} else {
					winston.info(`Launch promise ${playOptions.sid}`);
					var firstPromise = scenarioCollection.find({_id:new ObjectID(playOptions.sid)}).toArray();
					var secondPromise = amqp.connect(rmqUrl).then( conn => {return conn.createConfirmChannel()}).catch( e=> {return Promise.reject(e)});
					Promise.all([firstPromise,secondPromise]).then(promizesResults => {
						winston.info('Play Now Request ');
						var scenarioToPlay = promizesResults[0][0];
						var channel = promizesResults[1];
						var msg = JSON.stringify({ sid:scenarioToPlay._id , actions: scenarioToPlay.actions })
						winston.info(msg);
						channel.assertQueue(QUEUE_NAME, { durable: true })
						.then(ok => {
							if (ok) {
								return channel.sendToQueue(QUEUE_NAME, Buffer.from(msg), {persistent: true});
							} else {
								return Promise.reject(ok);
							}
						}).then(() => {
							channel.close();
							db.close();
							res.send(`play request sent for scenario ${playOptions.sid}`).status(200).end();
						});
					})
					.catch(err => {
						db.close();
						res.send(err).status(500).end();
					})
				}
			});
			
		}).catch(err => {
			winston.info(err);
			res.send(err).status(500).end;
		});
	}

	function addCron(req, res, playOptions) {
		const cron = playOptions.cron;
		winston.info(`Add new cron for scenario (${playOptions.sid}) with cron ${cron}`);
		if (cron) {
			Crontab.load(function(err, ct) {
				// create with string expression
				const NODE_BIN = '/usr/local/n/versions/node/8.4.0/bin/node';
				const NODE_SCRIPT = '/tmp/scheduler/sendPlayMessage.js';
				const NODE_OPTIONS = `--mongo=mongo --rabbit=rabbit --sid=${playOptions.sid}`;
				const LOG = `>> /var/log/watcron.log 2>&1`;
				var job = ct.create(`${NODE_BIN} ${NODE_SCRIPT} ${NODE_OPTIONS} ${LOG}`, cron);
				
				ct.save(function(err, newCrontab) {
					if (err) {
						res.send(err).status(500).end();
					}
					else {
						res.send(`cron request sent for scenario ${playOptions.sid}`).status(200).end();
					}
				}); 
			});
		} else {
			res.send(`cron ${cron} is not valid`).status(500).end();
		}
	}
};