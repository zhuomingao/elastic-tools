const elasticsearch         = require('elasticsearch');
const moment                = require('moment');
const nock                  = require('nock');
const path                  = require('path');
const winston               = require('winston');
const WinstonNullTransport  = require('winston-null-transport');

const ElasticTools = require('../index');

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.simple(),
    transports: [
        new WinstonNullTransport()
    ]
});

beforeAll(() => {
    nock.disableNetConnect();
})

//After each test, cleanup any remaining mocks
afterEach(() => {
    nock.cleanAll();
});

afterAll(() => {
    nock.enableNetConnect();
})

describe('ElasticTools', async() => {

    describe('createTimestampedIndex', async () => {

        it('creates the index', async () => {            
            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const aliasName = 'bryantestidx';
            const urlRegex = /\/(.*)/;
            let interceptedIdx = '';

            const scope = nock('http://example.org:9200')            
            .put(
                (uri) => {
                    //So we need to get the index name the function created, and in elasticsearch,
                    //that is the URI
                    const match = uri.match(urlRegex);
                    if (match) {
                        interceptedIdx = match[1];
                        return true;
                    } else {
                        return false;
                    }
                }, 
                {
                    settings: settings.settings,
                    mappings: mappings.mappings
                }
            )
            .reply(200, {"acknowledged":true,"shards_acknowledged":true,"index":interceptedIdx} );
      
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
      
            const estools = new ElasticTools(logger, client);
        
            const expectedTime = Date.now();
            const indexName = await estools.createTimestampedIndex(aliasName, mappings, settings);
            
            //Let's make sure the nock call was called
            expect(scope.isDone()).toBeTruthy();

            const dateRegex = /^bryantestidx_([\d]{4})([\d]{2})([\d]{2})_([\d]{2})([\d]{2})([\d]{2})$/;
            const matches = indexName.match(dateRegex);

            //Now the index name should match
            expect(matches).toBeTruthy();

            const [year, mon, day, hr, min, sec] = matches.slice(1).map(s => Number.parseInt(s));

            const actualTime = new Date(year, mon-1, day, hr, min, sec).getTime();

            //So the expected will never be the same as the actual time, so
            //lets just make sure it is within 5 seconds +/-
            expect(actualTime).toBeGreaterThanOrEqual(expectedTime - 5000);
            expect(actualTime).toBeLessThan(expectedTime + 5000);
        })

    });

    describe('createIndex', async () => {

        it('creates the index', async () => { 

            const now = moment();
            const timestamp = now.format("YYYYMMDD_HHmmss");
            const indexName = 'bryantestidx' + timestamp;
          
            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const scope = nock('http://example.org:9200')
                .put(`/${indexName}`, {
                    settings: settings.settings,
                    mappings: mappings.mappings
                })
                .reply(200, {"acknowledged":true,"shards_acknowledged":true,"index":indexName} );
          
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
          
            const estools = new ElasticTools(logger, client);
          
            await estools.createIndex(indexName, mappings, settings);

            expect(scope.isDone()).toBeTruthy();
        })

        it('handles index already exists', async() => {
            const now = moment();
            const timestamp = now.format("YYYYMMDD_HHmmss");
            const indexName = 'bryantestidx' + timestamp;
          
            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const scope = nock('http://example.org:9200')
                .put(`/${indexName}`, {
                    settings: settings.settings,
                    mappings: mappings.mappings
                })
                .reply(400, 
                    {
                        "error": {
                        "root_cause": [
                        {
                        "type": "index_already_exists_exception",
                        "reason": `index [${indexName}/EE0VmcmPT-q9MatbvD6vAw] already exists`,
                        "index_uuid": "EE0VmcmPT-q9MatbvD6vAw",
                        "index": indexName
                        }
                        ],
                        "type": "index_already_exists_exception",
                        "reason": `index [${indexName}/EE0VmcmPT-q9MatbvD6vAw] already exists`,
                        "index_uuid": "EE0VmcmPT-q9MatbvD6vAw",
                        "index": indexName
                        },
                        "status": 400
                    }
                );
          
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
                      
            const estools = new ElasticTools(logger, client);
          
            //TODO: Actually check and see if we get an logged error when the
            //exception occurs.
            try {
                await estools.createIndex(indexName, mappings, settings);
            } catch (err) {
                expect(err).not.toBeNull();
            }
            
            expect(scope.isDone()).toBeTruthy();
        })
    })

    describe('optimizeIndex', async () => {
        it("optimizes the index", async() => {
            const indexName = 'bryantestidx';
          
            const scope = nock('http://example.org:9200')
                .post(`/${indexName}/_forcemerge?max_num_segments=1`, body => true)
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});
          
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
          
            const estools = new ElasticTools(logger, client);
          
            await estools.optimizeIndex(indexName);
            
            expect(scope.isDone()).toBeTruthy();
        })

        it("handles a 504 response", async () => {

        });

        it("logs an error on server error", async () => {

        });

        //The following test simulate slow responses, something we have seen with Elasticsearch.
        //So we increase the timeout to 1.5 minutes for this request. These need to be tested upon change,
        //but it would slow down tests.
        /*
        it("optimizes the index with delay", async() => {
            const indexName = 'bryantestidx';
          
            const scope = nock('http://example.org:9200')
                .post(`/${indexName}/_forcemerge?max_num_segments=1`, body => true)
                .delay({
                    head: 89000
                })
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});
                
          
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
          
            const estools = new ElasticTools(logger, client);
          
            await estools.optimizeIndex(indexName);
            
            expect(scope.isDone()).toBeTruthy();
        }, 100000)

        it("optimizes the index with delay", async() => {
            const indexName = 'bryantestidx';
          
            const scope = nock('http://example.org:9200')
                .post(`/${indexName}/_forcemerge?max_num_segments=1`, body => true)
                .delay({
                    head: 95000
                })
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});
                
          
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
          
            const estools = new ElasticTools(logger, client);
          
            try {
                await estools.optimizeIndex(indexName);
            } catch (err) {
                expect(err).not.toBeNull();
            }
            
            expect(scope.isDone()).toBeTruthy();
        }, 110000)
        */
    })

    describe('getIndicesOlderThan', async () => {
        it('returns 1 when 1 is old', async() => {
            const indexName = 'bryantestidx';
            
            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1523901276157"}}}
                });
            
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            
            const expected = ["bryantestidx_1"];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);
            
            expect(indices).toEqual(expected);
        });

        it('returns 0 when 1 not old', async() => {
            const indexName = 'bryantestidx';
            
            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1525225677001"}}}
                });
            
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            
            const expected = [];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);
            
            expect(indices).toEqual(expected);
        });

        it('returns 2 in order when 2 of 3 are old', async() => {
            const indexName = 'bryantestidx';
            
            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1525225677001"}}},
                    "bryantestidx_2":{ "settings": {"index": {"creation_date":"1525225676001"}}},
                    "bryantestidx_3":{ "settings": {"index": {"creation_date":"1525225676002"}}}
                });
            
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            
            const expected = ['bryantestidx_3','bryantestidx_2'];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);
            
            expect(indices).toEqual(expected);
        });        

        it('handles server error', async () => {
            const indexName = 'bryantestidx';

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            try {
                const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);
            } catch (err) {
                expect(err).toBeTruthy();
            }
        });

        it('checks alias name', async () => {

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            try {
                const indices = await estools.getIndicesOlderThan();
            } catch (err) {
                expect(err).toBeTruthy();
            }                   
        });
        
    })


    describe('setAliasToSingleIndex', async () => {

        const aliasName = 'bryantestidx';        

        it ('adds without removing', async () => {

            const indexName = aliasName + "_1";

            //Setup nocks
            const scope = nock('http://example.org:9200');

            //Get Indices for Alias, not finding any
            scope.get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });
            //Update Alias
            scope.post(`/_aliases`, {
                    actions: [
                        { add: { indices: indexName, alias: aliasName } }
                    ]
                })
                .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.setAliasToSingleIndex(aliasName, indexName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('add one and removes one', async () => {
            const indexName = aliasName + "_1";
            const removeIndex = aliasName + "_2";
            //Setup nocks
            const scope = nock('http://example.org:9200');

            //Get Indices for Alias, not finding any
            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(200, {
                    [removeIndex]: {
                        "aliases": {
                            [aliasName]: {}
                        }
                    }
                });

            //Update Alias
            scope.post(`/_aliases`, {
                    actions: [
                        { add: { indices: indexName, alias: aliasName } },
                        { remove: { indices: [ removeIndex ], alias: aliasName } }
                    ]
                })
                .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.setAliasToSingleIndex(aliasName, indexName);

            expect(nock.isDone()).toBeTruthy();            
        })

    });

    describe('updateAlias', async () => {
        //Gonna use this a lot here, so set it once
        const aliasName = 'bryantestidx';

        it('checks for at least one add or remove', async ()=> {
            
            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            
            //No params
            try {
                await estools.updateAlias(aliasName);
            } catch(err) {
                expect(err).toMatchObject({
                    message: "You must add or remove at least one index"
                });
            }
            
            //One is not a string or array
            try {
                await estools.updateAlias(aliasName, { add: 1});
            } catch(err) {
                expect(err).toMatchObject({
                    message: "Indices to add must either be a string or an array of items"
                });
            }

            //One is not a string or array
            try {
                await estools.updateAlias(aliasName, { remove: 1});
            } catch(err) {
                expect(err).toMatchObject({
                    message: "Indices to remove must either be a string or an array of items"
                });
            }

        })

        it('adds one', async() => {

            const index = "myindex";

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { add: index});

            expect(nock.isDone()).toBeTruthy();

        })

        it('adds one arr', async() => {

            const index = [ "myindex" ];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { add: index});

            expect(nock.isDone()).toBeTruthy();

        })

        it('removes one arr', async() => {
            const index = [ "myindex" ];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { remove: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { remove: index});

            expect(nock.isDone()).toBeTruthy();            
        })

        it('swaps one for one', async() => {
            const add = "myindex";
            const remove = "myindex3";

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: add, alias: aliasName } },
                    { remove: { indices: remove, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { add, remove });

            expect(nock.isDone()).toBeTruthy();             
        })

        it('adds many', async() => {
            const indices = ["myindex", "myindex2"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { add: indices});

            expect(nock.isDone()).toBeTruthy();
            
        })

        it('removes many', async() => {
            const indices = ["myindex", "myindex2"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { remove: { indices, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { remove: indices});

            expect(nock.isDone()).toBeTruthy();            
        })

        it('swaps many for many', async() => {
            const add = ["myindex", "myindex2"];
            const remove = ["myindex3", "myindex4"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: add, alias: aliasName } },
                    { remove: { indices: remove, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            await estools.updateAlias(aliasName, { add, remove });

            expect(nock.isDone()).toBeTruthy();             
        })

        it('handles server error', async () => {

        });
    })

    describe('getIndicesForAlias', async () => {
        ///_alias/<%=name%>

        it('returns indices', async () => {
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';
            const indexName = aliasName + "_1";
            
            //This is the nock for getIndicesOlderThan
            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .get(`/_alias/${aliasName}`)
                .reply(200, {
                    [indexName]: {
                        "aliases": {
                           [aliasName]: {}
                        }
                    }
                });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
                        
            const expectedIndices = [indexName];
            const actualIndices = await estools.getIndicesForAlias(aliasName);

            //Check that all of our expected calls are done
            expect(nock.isDone()).toBeTruthy();
            
            //Check the data is as expected.
            expect(actualIndices).toEqual(expectedIndices);
        })

        if ('handles 404', async () => {
            const aliasName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });

                const client = new elasticsearch.Client({
                    host: 'http://example.org:9200',
                    apiVersion: '5.6'
                });            
                
            const estools = new ElasticTools(logger, client);
                        
            const expectedIndices = [];
            const actualIndices = await estools.getIndicesForAlias(aliasName);

            //Check that all of our expected calls are done
            expect(nock.isDone()).toBeTruthy();
            
            //Check the data is as expected.
            expect(actualIndices).toEqual(expectedIndices);
        });

        if ('handles Exception', async () => {
            const aliasName = 'bryantestidx';

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });

            const estools = new ElasticTools(logger, client);

            const expectedIndices = [];
            try {
                const actualIndices = await estools.getIndicesForAlias(aliasName);
            } catch (err) {
                expect(err).toBeTruthy();
            }
        });
    })

    describe('indexDocument', async () => {

    });

    describe('deleteIndex', async () => {

        it('deletes the index', async () => {
            const aliasName = 'bryantestidx';

            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .delete(`/${aliasName}_2`)
                .reply(200, {
                    "acknowledged": true
                })

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);

            await estools.deleteIndex(aliasName + "_2");

            expect(nock.isDone()).toBeTruthy();

        })

    });

    describe('cleanupOldIndices', async () => {
        it ('has nothing to clean', async () => {
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';
            
            //This is the nock for getIndicesOlderThan
            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date":now}}}
                });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);

            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('cleans up nothing because alias', async () => {
            
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';

            //Setup the first nock
            const scope = nock('http://example.org:9200');

            //Setup the first nock for getIndicesOlderThan
            scope.get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date": now }}}, //Should not be deleted
                    [aliasName + "_2"]:{ "settings": {"index": {"creation_date": moment(now).subtract(10, 'days').unix() }}} //Should not delete
                });

            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(200, {
                    [aliasName + "_2"]: {
                        "aliases": {
                            [aliasName]: {}
                        }
                    }
                });

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('cleans up indices', async () => {
            
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';

            //Setup the first nock
            const scope = nock('http://example.org:9200');

            //Setup the first nock for getIndicesOlderThan
            scope.get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date": now }}}, //Should not be deleted
                    [aliasName + "_2"]:{ "settings": {"index": {"creation_date": moment(now).subtract(10, 'days').unix() }}}, //Should delete
                    [aliasName + "_3"]:{ "settings": {"index": {"creation_date": moment(now).subtract(11, 'days').unix() }}} //Should delete
                });

            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });

            //Nock for delete.
            scope.delete(`/${aliasName}_2`)
                .reply(200, {
                    "acknowledged": true
                })
                .delete(`/${aliasName}_3`)
                .reply(200, {
                    "acknowledged": true
                })

            const client = new elasticsearch.Client({
                host: 'http://example.org:9200',
                apiVersion: '5.6'
            });            
            
            const estools = new ElasticTools(logger, client);
            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        })
    });

})