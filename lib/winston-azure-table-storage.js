/*!
 * winston-azure-table-storage
 * Copyright(c) 2017 Ramji Piramanayagam
 * Apache 2.0 Licensed
 */

var util = require('util'),
    Transport = require('winston-transport'),
    azure = require('azure-storage'),
    winston = require('winston');

module.exports = class WinstonAzureTable extends Transport {
    constructor(opts) {
        opts = opts || {};

        this.name = 'azure';
        this.tableName = options.table || 'log';
        this.level = options.level || 'info';
        this.silent = options.silent || false;
        this.metaAsColumns = options.metaAsColumns || false;
        this.partition = options.partition || 'log';
        this.rowKeyBuilder = options.rowKeyBuilder || function () {
            var rtext = '';
            var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
            for (var i = 0; i < 5; i++)
                rtext += possible.charAt(Math.floor(Math.random() * possible.length));
            return (new Date()).getTime() + '_' + (new Date()).getMilliseconds() + '_' + rtext;
        };
    
        if (options.host && options.sas) {
            this.tableService = azure.createTableServiceWithSas(options.host, options.sas);
        } else {
            this.tableService = azure.createTableService(options.account, options.key);
        }
        this.entityGenerator = azure.TableUtilities.entityGenerator;
        this.created = false;
    }

    createTableIfNotExists (callback) {
        var me = this;
        if (me.created) {
            callback();
            return;
        }
    
        me.tableService.createTableIfNotExists(me.tableName, function (error) {
            if (!error) {
                me.created = true;
            }
            callback(error);
        });
    }

    log (meta, callback) {

        var me = this;
    
        if (me.silent) {
            return callback();
        }
    
        var entity = {
            PartitionKey: me.entityGenerator.String(me.partition),
            RowKey: me.entityGenerator.String(me.rowKeyBuilder())
        };
    
        if (meta) {
            if (me.metaAsColumns) {
                for (var prop in meta) {
                    if (typeof meta[prop] === 'object') {
                        if (meta[prop].toJSON) {
                            entity[prop] = me.entityGenerator.String(meta[prop].toJSON());
                        } else {
                            entity[prop] = me.entityGenerator.String(JSON.stringify(meta[prop]));
                        }
                    } else {
                        entity[prop] = me.entityGenerator.String(meta[prop]);
                    }
                }
            }
            else {
                entity.meta = me.entityGenerator.String(JSON.stringify(meta));
            }
        }
    
        me.createTableIfNotExists(function(err) {
            if (err){
                callback(err);
                return;
            }
    
            me.tableService.insertEntity(me.tableName, entity, function (err) {
        
                if (err) {
                    callback(err);
                    return;
                }
        
                //self.emit('logged', rowKey);
        
        
                callback(null, entity.RowKey);
        
            });
        });
    }

    explore (limit, callback) {
        var me = this;
        
        var query = new azure.TableQuery()
            .top(limit || 10)
            .where('PartitionKey eq ?', me.partition);
    
            me.tableService.queryEntities(me.tableName, query, null, function (error, result, response) {
    
            if (error) {
                callback(error);
                return;
            }
    
            callback(null, result);
        });
    }
}

var WinstonAzureTable = exports.WinstonAzureTable = function (options) {

    winston.Transport.call(this, options);

    options = options || {};

    this.name = 'azure';
    this.tableName = options.table || 'log';
    this.level = options.level || 'info';
    this.silent = options.silent || false;
    this.metaAsColumns = options.metaAsColumns || false;
    this.partition = options.partition || 'log';
    this.rowKeyBuilder = options.rowKeyBuilder || function () {
        var rtext = '';
        var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        for (var i = 0; i < 5; i++)
            rtext += possible.charAt(Math.floor(Math.random() * possible.length));
        return (new Date()).getTime() + '_' + (new Date()).getMilliseconds() + '_' + rtext;
    };

    if (options.host && options.sas) {
        this.tableService = azure.createTableServiceWithSas(options.host, options.sas);
    } else {
        this.tableService = azure.createTableService(options.account, options.key);
    }
    this.entityGenerator = azure.TableUtilities.entityGenerator;
    this.created = false;
};

winston.transports.AzureTable = WinstonAzureTable;