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
    constructor(options) {
        super(options);
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
    }

    createTableIfNotExists(callback) {
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

    log(info, callback) {

        var me = this;

        if (me.silent) {
            return callback();
        }

        var entity = {
            PartitionKey: me.entityGenerator.String(me.partition),
            RowKey: me.entityGenerator.String(me.rowKeyBuilder()),
            level: me.entityGenerator.String(info['level']),
            message: me.entityGenerator.String(info['message'])
        };

        if (info.meta) {
            if (me.metaAsColumns) {
                for (var prop in info.meta) {
                    if (typeof info.meta[prop] === 'object') {
                        if (info.meta[prop] && info.meta[prop].toJSON) {
                            entity[prop] = me.entityGenerator.String(info.meta[prop].toJSON());
                        } else {
                            entity[prop] = me.entityGenerator.String(JSON.stringify(info.meta[prop]));
                        }
                    } else {
                        entity[prop] = me.entityGenerator.String(info.meta[prop]);
                    }
                }
            }
            else {
                entity.meta = me.entityGenerator.String(JSON.stringify(info.meta));
            }
        }

        me.createTableIfNotExists(function (err) {
            if (err) {
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

    explore(limit, callback) {
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

winston.transports.AzureTable = module.exports;