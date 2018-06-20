const moment                = require('moment');

/**
 * This class defines a wrapper around the elasticsearch framework.
 */
class ElasticTools {

    /**
     * Creates a new instance of the ElasticTools
     * @param {Object} logger The logger to use for logging
     * @param {Object} elasticClient The elasticsearch client to use.
     */
    constructor(logger, elasticClient) {
        this.logger = logger;
        this.client = elasticClient;
    }

    /**
     * Creates a new index with the name, mapping and settings
     * @param {string} indexName the name of the index
     * @param {Object} mapping the index mapping (fields, types, etc)
     * @param {Object} settings the index settings (shards, replicas, analyzers, etc)
     */
    async createIndex(indexName, mapping, settings) {
        let createResponse;

        try {
            createResponse = await this.client.indices.create({
                index: indexName,
                body: {
                    ...settings,
                    ...mapping
                }
            });
        } catch (err) {
            this.logger.error(`Could not create index ${indexName}. ${err.message}`);
            throw err;
        }

        //NOTE: the response could indicate that the index creation did not 
        //complete before the timeout. We need to identify when that will actually happen.        
    }

    /**
     * Creates an index with a timestamp. Used for loaders that create indicies and then 
     * swap aliases upon successful completion.
     * @param {*} name The index name prefix
     * @param {Object} mappings the index mapping (fields, types, etc)
     * @param {Object} settings the index settings (shards, replicas, analyzers, etc)
     */
    async createTimestampedIndex(name, mappings, settings) {
        const now = moment();
        const timestamp = now.format("YYYYMMDD_HHmmss");
        const indexName = `${name}_${timestamp}`;
        await this.createIndex(indexName, mappings, settings);
        return indexName;
    }

    /**
     * Performs a merge operation on the index to ensure cluster maintains a consistent state
     * @param {string} indexName the name of the index to optimize
     */
    async optimizeIndex(indexName) {

        try {
            await this.client.indices.forcemerge({
                maxNumSegments: 1,
                index: indexName,
                requestTimeout: 90000 //Merges can be slow for big indexes.
            })
        } catch(err) {
            if (err.statusCode !== 504) {
                this.logger.error(`Could not optimize index ${indexName}`)
            }
        }
    }

    /**
     * Deletes an index
     * @param {*} indexName the name of the index to delete
     */
    async deleteIndex(indexName) {        

        try {
            await this.client.indices.delete({
                index: indexName
            })
        } catch(err) {
            this.logger.error(`Could not delete index ${indexName}`);
            throw err;
        }        
        
    }

    /**
     * Points an alias to a new index name.  This will remove all other
     * indices from the alias.  
     * @param {*} aliasName 
     * @param {*} indexName 
     */
    async setAliasToSingleIndex(aliasName, indexName) {
        //Get indices for aliases
        try {
            const assocIndices = await this.getIndicesForAlias(aliasName);
            const removeIdx = assocIndices.filter(idx => idx !== indexName);
            
            await this.updateAlias(aliasName, {
                add: indexName,
                remove: removeIdx
            });
        } catch (err) {
            this.logger.error(`Could not set alias, ${aliasName}, to index, ${indexName}`)
            throw err;
        }
    }

    /**
     * Gets a list of indices matching an aliasName for when indices are named using the
     * <aliasName>_<timestamp> format.
     * @param {string} aliasName The root name for the indices..  Must not be empty.
     * @param {Date} datetime 
     */
    async getIndicesOlderThan(aliasName, datetime) {

        if (!aliasName) {
            throw new Error("aliasName cannot be null");
        }

        let res;

        try {
            res = await this.client.indices.getSettings({
                index: (aliasName + '*'),
                name: "index.creation_date" //Only get creation date field
            })
        } catch (err) {
            this.logger.error(`Could not get indices older than ${datetime} for pattern ${aliasName}*`);
            throw err;
        }

        const indices = Object.keys(res);
        const older = indices
            .filter(idx => res[idx].settings.index.creation_date < datetime)
            .sort((a,b) => {  
                const adate = res[a].settings.index.creation_date;
                const bdate = res[b].settings.index.creation_date;
                return bdate - adate;
            });

        return older;
    }

    /**
     * Updates an alias by adding and removing indices
     * @param {*} aliasName The alias name to update
     * @param {*} param1 
     * @param {(string|string[])} param1.add A single index name or an array of indices to add to the alias
     * @param {(string|string[])} param1.remove A single index name or an array of indices to remove from the alias 
     */
    async updateAlias(aliasName, { add = [] , remove = [] } = {}) {

        let addArr = [];
        if (add && (typeof add === 'string' || Array.isArray(add))) {
            addArr = add === 'string' ? [add] : add;
        } else if (add) {            
            throw new Error("Indices to add must either be a string or an array of items")
        } //Else it is empty and that is ok.

        let removeArr = []
        if (remove && (typeof remove === 'string' || Array.isArray(remove)) ) {
            removeArr = remove === 'string' ? [remove] : remove;
        } else if (remove) {            
            throw new Error("Indices to remove must either be a string or an array of items")
        }
        
        if (!addArr.length && !removeArr.length) {
            throw new Error("You must add or remove at least one index");
        }

        let actions = [];
        if (addArr.length) {
            actions.push({
                "add": { "indices": addArr, "alias": aliasName }
            })
        }
        
        if (removeArr.length) {
            actions.push({
                "remove": { "indices": removeArr, "alias": aliasName }
            })
        }

        // Just gets back { acknowledged: true }
        try {
            await this.client.indices.updateAliases({
                body : {
                    actions
                }
            })
        } catch (err) {
            this.logger.error(`Could not update alias: ${aliasName}`)
            throw err;
        }
    }


    /**
     * Gets the indices associated with the alias.
     * @param {*} aliasName the name of the alias
     */
    async getIndicesForAlias(aliasName) {

        let res;

        try {
            res = await this.client.indices.getAlias({
                name: aliasName
            })
        } catch(err) {

            // There are no indices. so just return
            if (err.status == 404) {
                return [];
            }

            this.logger.error(`Could not get aliases for ${aliasName}`);
            throw(err);
        }

        return Object.keys(res);
    }
    
    /**
     * Index a single document
     * @param {string} indexName the name of the index to store the document
     * @param {string} type the document to store
     * @param {string} id the unique ID of the document
     * @param {Object} document the document to store
     */
    async indexDocument(indexName, type, id, document) {
        try {
            await this.client.index({
                index: indexName,
                type,
                id,
                body: document
            })
        } catch(err) {
            this.logger.error(`Could not index document ${id} for index ${indexName}`);
            throw err;
        }
    }

    /**
     * Cleans up all the old unused indices. Always at least one is kept.
     * @param {string} indexPrefix The prefix for the timestamped indices. (Usually the alias name)
     * @param {Number} daysToKeep The number of days to keep (Default: 5)
     * @param {Number} minIndexesToKeep The minimum number of indexes to keep. (Default: 0) (Not Implemented)
     */
    async cleanupOldIndices(indexPrefix, daysToKeep = 5, minIndexesToKeep = 0) {
        //Setup time for the old date.
        const olderThanDate = moment().subtract(daysToKeep, 'days').startOf('day').valueOf();

        //Get all the indices older than our cutoff.            
        const oldIndices = await this.getIndicesOlderThan(indexPrefix, olderThanDate);

        //If there are no indices, then move on. No sense calling more services
        if (oldIndices.length === 0) {
            return;
        }

        //Get all the indices for our alias
        const aliasedIndices = await this.getIndicesForAlias(indexPrefix);

        //Since we should not removed indices that our currently used by the alias,
        //remove them from the list.
        const indicesToDelete = oldIndices.filter((idx) => !aliasedIndices.includes(idx));
        
        //Now remove them.
        if (indicesToDelete) {
            await Promise.all(
                indicesToDelete.map(
                    async (name) => {
                        await this.deleteIndex(name);
                    }
                )
            );
        }
    }

    //TODO: indexBulk

}

module.exports = ElasticTools;

