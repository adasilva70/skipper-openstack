var pkgcloud = require('pkgcloud-bluemix-objectstorage'),
    Writable = require("stream").Writable,
    _ = require("underscore");

function getClient(credentials) {
	// Create a config object
    var config = {};

	// Specify Openstack as the provider
    config.provider = "openstack";

	// Authentication url
    config.authUrl = 'https://identity.open.softlayer.com/';
    config.region= credentials.region;

	// Use the service catalog
    config.useServiceCatalog = true;

	// true for applications running inside Bluemix, otherwise false
    config.useInternal = false;

	// projectId as provided in your Service Credentials
    config.tenantId = credentials.projectId;

	// userId as provided in your Service Credentials
    config.userId = credentials.userId;

	// username as provided in your Service Credentials
    config.username = credentials.username;

	// password as provided in your Service Credentials
    config.password = credentials.password;

	// This is part which is NOT in original pkgcloud. This is how it works with newest version of bluemix and pkgcloud at 22.12.2015. 
	//In reality, anything you put in this config.auth will be send in body to server, so if you need change anything to make it work, you can. PS : Yes, these are the same credentials as you put to config before. 
	//I do not fill this automatically to make it transparent.

	config.auth = {
    	forceUri  : "https://identity.open.softlayer.com/v3/auth/tokens", //force uri to v3, usually you take the baseurl for authentication and add this to it /v3/auth/tokens (at least in bluemix)    
    	interfaceName : "public", //use public for apps outside bluemix and internal for apps inside bluemix. There is also admin interface, I personally do not know, what it is for.
    	"identity": {
        	"methods": [
            	"password"
        	],
        	"password": {
            	"user": {
                	"id": credentials.userId, //userId
                	"password": credentials.password //userPassword
            	}
        	}
    	},
    	"scope": {
        	"project": {
            	"id": credentials.projectId //projectId
        	}
    	}
	};

    console.log("config: " + JSON.stringify(config));

	// Create a pkgcloud storage client
    var storageClient = pkgcloud.storage.createClient(config);

	// Authenticate to OpenStack
     storageClient.auth(function (error) {
        if (error) {
            console.error("storageClient.auth() : error creating storage client: ", error);
        }
        else {
            // Print the identity object which contains your Keystone token.
            console.log("storageClient.auth() : created storage client: " + JSON.stringify(storageClient._identity));
        }

    });
    
    return storageClient;
}

module.exports = function SwiftStore(globalOpts) {
    globalOpts = globalOpts || {};

    var adapter = {

        read: function(options, file, response) {
            var client = getClient(options.credentials);
            client.download({
                container: options.container,
                remote: file,
                stream: response
            });
        },

        rm: function(fd, cb) { return cb(new Error('TODO')); },

        ls: function(options, callback) {
            var client = getClient(options.credentials);

            client.getFiles(options.container, function (error, files) {
                return callback(error, files);
            });
        },

        receive: function(options) {
            var receiver = Writable({
                objectMode: true
            });

            receiver._write = function onFile(__newFile, encoding, done) {
                var client = getClient(options.credentials);
                console.log("Uploading file with name", __newFile.filename);
                __newFile.pipe(client.upload({
                    container: options.container,
                    remote: __newFile.filename
                }, function(err, value) {
                  console.log(err);
                  console.log(value);
                    if( err ) {
                      console.log( err);
                      receiver.emit( 'error', err );
                      return;
                    }
                    done();
                }));

                __newFile.on("end", function(err, value) {
                  console.log("finished uploading", __newFile.filename);
                    receiver.emit('finish', err, value );
                    done();
                });

            };

            return receiver;
        },
        ensureContainerExists: function(credentials, containerName, callback) {
          var client = getClient(credentials);

          client.getContainers(function (error, containers) {
            if (error) {
              callback(error);
              return;
            }
            if (containers.length === 0) {
              client.createContainer(containerName, callback);
            }
            else {
              var found = _.find(containers, function (container) {
                  return container.name === containerName;
              });
              if (found === undefined) {
                client.createContainer(containerName, callback);
              }
              else {
                callback(null);
              }
            }

          });
        }
    }

  return adapter;
}
