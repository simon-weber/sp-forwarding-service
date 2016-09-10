'use strict';

let q = require('q')
  , express = require('express')
  , app = express()
  , bodyParser = require('body-parser')
  , SparkPost = require('sparkpost')
  , sp = new SparkPost(process.env.SPARKPOST_API_KEY)
  , redis = require('redis')
  , subscriber = redis.createClient(process.env.REDIS_URL, {no_ready_check: true})
  , publisher = redis.createClient(process.env.REDIS_URL, {no_ready_check: true})
  , subscriberReady = false
  , publisherReady = false
  ;

/*
 * Check the environment/config vars are set up correctly
 */

if (process.env.SPARKPOST_API_KEY === undefined) {
  console.error('SPARKPOST_API_KEY must be set');
  process.exit(1);
}

if (process.env.FORWARD_FROM === undefined) {
  console.error('FORWARD_FROM must be set');
  process.exit(1);
}

if (process.env.FORWARD_TO === undefined) {
  console.error('FORWARD_TO must be set');
  process.exit(1);
}

/*
 * Set up the Redis publish/subscribe queue for incoming messages
 */

subscriber.on('error', function(err) {
  console.error('subscriber: ' + err);
  subscriberReady = false;
});

publisher.on('error', function(err) {
  console.error('publisher: ' + err);
  publisherReady = false;
});

subscriber.on('ready', function() {
  subscriberReady = true;
});

publisher.on('ready', function() {
  publisherReady = true;
});

subscriber.subscribe('queue');

subscriber.on('message', function(channel, message) {
  sp.transmissions.send({
    transmissionBody: {
      content: {
        email_rfc822: message
      },
      recipients: [{address: {email: process.env.FORWARD_TO}}]
    }
  }, function(err, res) {
    if (err) {
      console.error('Transmission failed: ' + JSON.stringify(err));
    } else {
      console.log('Transmission succeeded: ' + JSON.stringify(res.body));
    }
  });
});

/*
 * Set up Express
 */

app.set('port', (process.env.PORT || 5000));

app.use(express.static(__dirname + '/public'));

app.use(bodyParser.json());

// Default of 100k might be too small for many attachments
app.use(bodyParser.json({limit: '10mb'}));

/*
 * GET /inbound-webhook -- use the request object to find out where this
 * endpoint is being served from and use that to work out what the inbound
 * webhook endpoint should be. Get the list of inbound webhooks from SparkPost
 * and look for this one, returning it with the inbound domain.
 */

app.get('/inbound-webhook', function(request, response) {
  let appUrl = 'https://' + request.hostname + '/message';
  getInboundWebhooks()
    .then(function(webhooks) {
      let domain = null;
      for (var i in webhooks) {
        if (webhooks[i].target === appUrl) {
          domain = webhooks[i].match.domain;
          break;
        }
      }
      if (domain === null) {
        return response.sendStatus(404);
      }
      return response.status(200).json({app_url: appUrl, domain: domain });
    })
    .fail(function(msg) {
      return response.status(500).json({error: msg});
    });
});

/*
 * POST /inbound-webhook -- use the request object to find out where this
 * endpoint is being served from and use that to work out what the inbound
 * webhook endpoint should be. Then set that up in SparkPost using the given
 * domain.
 */

app.post('/inbound-webhook', function(request, response) {
  let domain;
  try {
    let data = JSON.parse(JSON.stringify(request.body));
    domain = data.domain;
  } catch (e) {
    return response.status(400).json({err: 'Invalid data'});
  }

  let appUrl = 'https://' + request.hostname + '/message';
  addInboundWebhook(appUrl, domain)
    .then(function() {
      return response.status(200).json({app_url: appUrl});
    })
    .fail(function(msg) {
      return response.status(500).json({error: msg});
    });
});

/*
 * POST /inbound-domain -- set up the given domain as an inbound domain in
 * SparkPost.
 */

app.post('/inbound-domain', function(request, response) {
  let domain;
  try {
    let data = JSON.parse(JSON.stringify(request.body));
    domain = data.domain;
  } catch (e) {
    return response.status(400).json({err: 'Invalid data'});
  }

  if (!domain) {
    return response.status(422).send('Missing domain');
  }

  addInboundDomain(domain)
    .then(function() {
      return response.status(200).json({domain: domain});
    })
    .fail(function(msg) {
      return response.status(500).send(msg);
    });
});

/*
 * POST /message -- this is the webhook endpoint. Messages received from
 * SparkPost are put on a Redis queue for later processing, so that 200 can be
 * returned immediately.
 */

app.post('/message', function(request, response) {
  if (!subscriberReady || !publisherReady) {
    return response.status(500).send('Not ready');
  }

  try {
    let data = JSON.parse(JSON.stringify(request.body))
      // The From: address needs to be changed to use a verified domain,
      // and Sender removed (it could be changed as well, but that would make
      // it redundant).
      // Note that jshint fails here due to a bug (https://github.com/jshint/jshint/pull/2881)
      , message = data[0].msys.relay_message.content.email_rfc822
        .replace(/^From: .*$/m, 'From: ' + process.env.FORWARD_FROM)
        .replace(/^Sender: .*$/m, '');

    publisher.publish('queue', message);

    return response.status(200).send('OK');
  } catch (e) {
    return response.status(400).send('Invalid data');
  }
});

/*
 * Helper functions
 */

function addInboundDomain(domain) {
  return q.Promise(function(resolve, reject) {
    sp.inboundDomains.create({domain: domain}, function(err) {
      if (err) {
        console.error(domain, err);
        reject(err);
      } else {
        console.log('Inbound domain ' + domain + ' created');
        resolve();
      }
    });
  });
}

function getInboundWebhooks() {
  return q.Promise(function(resolve, reject) {
    sp.relayWebhooks.all(function(err, data) {
      if (err) {
        reject(err);
      } else {
        resolve(JSON.parse(data.body).results);
      }
    });
  });
}

function addInboundWebhook(appUrl, domain) {
  return q.Promise(function(resolve, reject) {
    sp.relayWebhooks.create({
      target: appUrl,
      domain: domain,
      name: 'Forwarding Service',
      auth_token: '1234567890qwertyuio', // TODO do this properly
      protocol: 'SMTP'
    }, function(err) {
      if (err) {
        reject(err);
      } else {
        console.log('Inbound webhook created');
        resolve();
      }
    });
  });
}

app.listen(app.get('port'), function() {
  console.log('Node app is running on port', app.get('port'));
});
