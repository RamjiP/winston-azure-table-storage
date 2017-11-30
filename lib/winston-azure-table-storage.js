/*!
 * winston-azure-table-storage
 * Copyright(c) 2017 Ramji Piramanayagam
 * Apache 2.0 Licensed
 */

var util = require('util'),
    azure = require('azure-storage'),
    winston = require('winston')

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
};

util.inherits(WinstonAzureTable, winston.Transport);

winston.transports.AzureTable = WinstonAzureTable;

WinstonAzureTable.prototype.createTableIfNotExists = function (callback) {
    this.tableService.createTableIfNotExists(this.tableName, function (error) {
        callback(error);
    });
};

WinstonAzureTable.prototype.log = function (meta, callback) {

    if (this.silent) {
        return callback(true);
    }

    var entity = {
        PartitionKey: this.entityGenerator.String(this.partition),
        RowKey: this.entityGenerator.String(this.rowKeyBuilder())
    };

    if (meta) {
        if (this.metaAsColumns) {
            for (var prop in meta) {
                if (typeof meta[prop] === 'object') {
                    entity[prop] = this.entityGenerator.String(JSON.stringify(meta[prop]));
                } else {
                    entity[prop] = this.entityGenerator.String(meta[prop]);
                }
            }
        }
        else {
            entity.meta = this.entityGenerator.String(JSON.stringify(meta));
        }
    }

    this.tableService.insertEntity(this.tableName, entity, function (err) {

        if (err) {
            throw err;
        }

        //self.emit('logged', rowKey);


        callback(null, entity.RowKey);

    });
};

WinstonAzureTable.prototype.explore = function (limit, callback) {
    var query = new azure.TableQuery()
        .top(limit || 10)
        .where('PartitionKey eq ?', this.partition);

    this.tableService.queryEntities(this.tableName, query, null, function (error, result, response) {

        if (error) {
            throw error;
        }

        callback(result);
    });
};